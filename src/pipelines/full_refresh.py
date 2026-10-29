"""
Strategy 1: Full Refresh Pipeline.
Replaces the entire target dataset atomically using a staging table swap.
Provides zero-downtime consistency, absolute idempotency, and drift self-healing.
"""
import duckdb
from typing import List, Dict, Any
from config.settings import TABLE_FULL_REFRESH
from src.schemas.order import OrderProcessedSchema, get_orders_table_ddl
from src.pipelines.base import BasePipeline

class FullRefreshPipeline(BasePipeline):
    """
    Full Refresh strategy:
    - Reads entire input dataset
    - Populates isolated staging table
    - Executes atomic swap with production table
    - Self-heals against missing or corrupted rows in target
    - Runtime cost scales linearly O(N) with entire historical data volume
    """

    def __init__(self, con: duckdb.DuckDBPyConnection, target_table: str = TABLE_FULL_REFRESH):
        super().__init__(con, target_table)
        self.con.execute(get_orders_table_ddl(self.target_table))

    def execute_load(
        self,
        valid_records: List[OrderProcessedSchema],
        run_id: str,
        source_file: str
    ) -> Dict[str, int]:
        """
        Loads records into a staging table and atomically replaces target table.
        """
        staging_table = f"{self.target_table}_staging_{run_id.replace('-', '_')}"
        backup_table = f"{self.target_table}_old_{run_id.replace('-', '_')}"

        # 1. Create fresh staging table
        self.con.execute(f"DROP TABLE IF EXISTS {staging_table};")
        self.con.execute(get_orders_table_ddl(staging_table))

        # 2. Insert records into staging table
        if valid_records:
            # Efficient PyArrow batch insert into DuckDB
            import pyarrow as pa
            from src.schemas.order import ORDER_COLUMNS
            records_dict_list = [r.to_ordered_dict() for r in valid_records]

            arrow_table = pa.Table.from_pylist(records_dict_list)
            # Register virtual Arrow view and insert with explicit columns
            cols = ", ".join(ORDER_COLUMNS)
            self.con.register("temp_arrow_view", arrow_table)
            self.con.execute(f"INSERT INTO {staging_table} ({cols}) SELECT {cols} FROM temp_arrow_view;")
            self.con.unregister("temp_arrow_view")

        # 3. Atomic swap
        # DuckDB supports table swap inside transaction
        self.con.execute("BEGIN TRANSACTION;")
        try:
            # Check if target table exists
            exists = self.con.execute(f"""
                SELECT count(*) FROM information_schema.tables 
                WHERE table_name = '{self.target_table}'
            """).fetchone()[0] > 0

            if exists:
                self.con.execute(f"DROP TABLE IF EXISTS {backup_table};")
                self.con.execute(f"ALTER TABLE {self.target_table} RENAME TO {backup_table};")
                self.con.execute(f"ALTER TABLE {staging_table} RENAME TO {self.target_table};")
                self.con.execute(f"DROP TABLE IF EXISTS {backup_table};")
            else:
                self.con.execute(f"ALTER TABLE {staging_table} RENAME TO {self.target_table};")

            self.con.execute("COMMIT;")
        except Exception:
            self.con.execute("ROLLBACK;")
            # Cleanup staging
            self.con.execute(f"DROP TABLE IF EXISTS {staging_table};")
            raise

        inserted_count = len(valid_records)
        return {
            "inserted": inserted_count,
            "updated": 0,
            "deleted": 0
        }
