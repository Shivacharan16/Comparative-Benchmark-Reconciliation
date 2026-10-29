"""
Strategy 3: Partition Replacement Pipeline.
Replaces target table data on a partition-by-partition basis (partition key: order_date).
Isolates blast radius to touched partitions while leaving historical partitions untouched.
"""
import duckdb
from typing import List, Dict, Any, Set
import pyarrow as pa
from config.settings import TABLE_PARTITION_REPLACE
from src.schemas.order import OrderProcessedSchema, get_orders_table_ddl
from src.pipelines.base import BasePipeline

class PartitionReplacementPipeline(BasePipeline):
    """
    Partition Replacement strategy:
    - Partition Key: order_date (e.g. YYYY-MM-DD)
    - Identifies distinct partition values in the incoming batch.
    - Reads existing partition rows for only the touched dates.
    - Merges incoming batch records with existing partition state (respecting watermark updated_at).
    - Atomically deletes and replaces ONLY the touched partitions:
        DELETE FROM target WHERE order_date IN (...)
        INSERT INTO target SELECT * FROM consolidated_staging
    - Historical untouched partitions are neither scanned nor rewritten.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection, target_table: str = TABLE_PARTITION_REPLACE):
        super().__init__(con, target_table)
        self.con.execute(get_orders_table_ddl(self.target_table))

    def execute_load(
        self,
        valid_records: List[OrderProcessedSchema],
        run_id: str,
        source_file: str
    ) -> Dict[str, int]:
        """
        Executes atomic partition replacement for all touched dates in valid_records.
        """
        if not valid_records:
            return {"inserted": 0, "updated": 0, "deleted": 0}

        staging_table = f"staging_part_{run_id.replace('-', '_')}"
        consolidated_table = f"staging_part_cons_{run_id.replace('-', '_')}"

        self.con.execute(f"DROP TABLE IF EXISTS {staging_table};")
        self.con.execute(f"DROP TABLE IF EXISTS {consolidated_table};")
        self.con.execute(get_orders_table_ddl(staging_table))
        self.con.execute(get_orders_table_ddl(consolidated_table))

        # 1. Stage incoming batch records with explicit column ordering
        from src.schemas.order import ORDER_COLUMNS
        records_dict_list = [r.to_ordered_dict() for r in valid_records]

        arrow_table = pa.Table.from_pylist(records_dict_list)
        cols = ", ".join(ORDER_COLUMNS)
        self.con.register("temp_part_arrow_view", arrow_table)
        self.con.execute(f"INSERT INTO {staging_table} ({cols}) SELECT {cols} FROM temp_part_arrow_view;")
        self.con.unregister("temp_part_arrow_view")

        # 2. Extract touched partition dates
        touched_dates_res = self.con.execute(f"SELECT DISTINCT CAST(order_date AS VARCHAR) FROM {staging_table}").fetchall()
        touched_dates = [r[0] for r in touched_dates_res]

        if not touched_dates:
            return {"inserted": 0, "updated": 0, "deleted": 0}

        formatted_dates = ", ".join(f"'{d}'" for d in touched_dates)

        # 3. Consolidate: Merge existing partition data with incoming batch
        # For untouched records in touched partitions: retain existing
        # For incoming records: update if newer, insert if new
        cols = ", ".join(ORDER_COLUMNS)
        consolidation_sql = f"""
        INSERT INTO {consolidated_table} ({cols})
        WITH existing_partition_data AS (
            SELECT {cols} FROM {self.target_table}
            WHERE CAST(order_date AS VARCHAR) IN ({formatted_dates})
        ),
        combined AS (
            -- Retain existing rows not updated by incoming batch
            SELECT {", ".join(["e." + c for c in ORDER_COLUMNS])}
            FROM existing_partition_data e
            LEFT JOIN {staging_table} s ON e.order_id = s.order_id
            WHERE s.order_id IS NULL

            UNION ALL

            -- Take incoming batch row if it is new, OR if it is newer than existing
            SELECT {", ".join(["s." + c for c in ORDER_COLUMNS])}
            FROM {staging_table} s
            LEFT JOIN existing_partition_data e ON s.order_id = e.order_id
            WHERE e.order_id IS NULL OR s.updated_at >= e.updated_at

            UNION ALL

            -- Retain existing row if incoming batch row was stale (out of order)
            SELECT {", ".join(["e." + c for c in ORDER_COLUMNS])}
            FROM existing_partition_data e
            JOIN {staging_table} s ON e.order_id = s.order_id
            WHERE s.updated_at < e.updated_at
        )
        SELECT DISTINCT {cols} FROM combined;
        """

        self.con.execute(consolidation_sql)

        # Calculate metrics
        existing_matched_count = self.con.execute(f"""
            SELECT count(*) FROM {staging_table} s
            JOIN {self.target_table} t ON s.order_id = t.order_id
            WHERE CAST(t.order_date AS VARCHAR) IN ({formatted_dates})
              AND s.updated_at >= t.updated_at
        """).fetchone()[0]

        new_inserted_count = self.con.execute(f"""
            SELECT count(*) FROM {staging_table} s
            LEFT JOIN {self.target_table} t ON s.order_id = t.order_id
            WHERE t.order_id IS NULL
        """).fetchone()[0]

        # 4. Atomic Partition Swap: Delete touched partitions and insert consolidated data
        self.con.execute("BEGIN TRANSACTION;")
        try:
            self.con.execute(f"DELETE FROM {self.target_table} WHERE CAST(order_date AS VARCHAR) IN ({formatted_dates});")
            self.con.execute(f"INSERT INTO {self.target_table} SELECT * FROM {consolidated_table};")
            self.con.execute("COMMIT;")
        except Exception:
            self.con.execute("ROLLBACK;")
            raise
        finally:
            self.con.execute(f"DROP TABLE IF EXISTS {staging_table};")
            self.con.execute(f"DROP TABLE IF EXISTS {consolidated_table};")

        return {
            "inserted": new_inserted_count,
            "updated": existing_matched_count,
            "deleted": 0
        }
