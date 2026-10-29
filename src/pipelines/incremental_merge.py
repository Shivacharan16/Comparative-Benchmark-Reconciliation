"""
Strategy 2: Incremental Merge Pipeline (UPSERT / CDC).
Updates existing records and inserts new records using SQL MERGE with watermark protection.
Ensures O(Delta N) runtime efficiency and robust out-of-order event handling.
"""
import duckdb
from typing import List, Dict, Any
import pyarrow as pa
from config.settings import TABLE_INCREMENTAL_MERGE
from src.schemas.order import OrderProcessedSchema, get_orders_table_ddl
from src.pipelines.base import BasePipeline

class IncrementalMergePipeline(BasePipeline):
    """
    Incremental Merge strategy:
    - Loads only delta records into staging table
    - Performs SQL MERGE INTO target using primary key (order_id)
    - Enforces watermark condition: src.updated_at >= target.updated_at
      to prevent stale / out-of-order events from overwriting fresh data
    - High efficiency O(delta) since only changed rows are processed
    """

    def __init__(self, con: duckdb.DuckDBPyConnection, target_table: str = TABLE_INCREMENTAL_MERGE):
        super().__init__(con, target_table)
        self.con.execute(get_orders_table_ddl(self.target_table))

    def execute_load(
        self,
        valid_records: List[OrderProcessedSchema],
        run_id: str,
        source_file: str
    ) -> Dict[str, int]:
        """
        Executes SQL MERGE INTO against the target table.
        """
        if not valid_records:
            return {"inserted": 0, "updated": 0, "deleted": 0}

        staging_table = f"staging_merge_{run_id.replace('-', '_')}"
        self.con.execute(f"DROP TABLE IF EXISTS {staging_table};")
        self.con.execute(get_orders_table_ddl(staging_table))

        # Insert records into staging table via PyArrow with explicit column ordering
        from src.schemas.order import ORDER_COLUMNS
        records_dict_list = [r.to_ordered_dict() for r in valid_records]

        arrow_table = pa.Table.from_pylist(records_dict_list)
        cols = ", ".join(ORDER_COLUMNS)
        self.con.register("temp_merge_arrow_view", arrow_table)
        self.con.execute(f"INSERT INTO {staging_table} ({cols}) SELECT {cols} FROM temp_merge_arrow_view;")
        self.con.unregister("temp_merge_arrow_view")

        # Determine pre-existing matching keys to calculate insert vs update metrics
        matched_keys_query = f"""
            SELECT count(*) FROM {staging_table} s
            JOIN {self.target_table} t ON s.order_id = t.order_id
            WHERE s.updated_at >= t.updated_at
        """
        updated_count = self.con.execute(matched_keys_query).fetchone()[0]

        new_keys_query = f"""
            SELECT count(*) FROM {staging_table} s
            LEFT JOIN {self.target_table} t ON s.order_id = t.order_id
            WHERE t.order_id IS NULL
        """
        inserted_count = self.con.execute(new_keys_query).fetchone()[0]

        # In DuckDB / SQL standard: MERGE statement
        # Check DuckDB version MERGE support or native ANSI SQL MERGE
        merge_sql = f"""
        MERGE INTO {self.target_table} AS target
        USING {staging_table} AS src
        ON target.order_id = src.order_id
        WHEN MATCHED AND src.updated_at >= target.updated_at THEN
            UPDATE SET
                customer_id = src.customer_id,
                order_date = src.order_date,
                order_timestamp = src.order_timestamp,
                order_status = src.order_status,
                payment_method = src.payment_method,
                currency = src.currency,
                amount = src.amount,
                tax_amount = src.tax_amount,
                total_amount = src.total_amount,
                items_count = src.items_count,
                updated_at = src.updated_at,
                is_deleted = src.is_deleted,
                run_id = src.run_id,
                source_file = src.source_file,
                ingested_at = src.ingested_at
        WHEN NOT MATCHED THEN
            INSERT (
                order_id, customer_id, order_date, order_timestamp, order_status,
                payment_method, currency, amount, tax_amount, total_amount,
                items_count, updated_at, is_deleted, run_id, source_file, ingested_at
            ) VALUES (
                src.order_id, src.customer_id, src.order_date, src.order_timestamp,
                src.order_status, src.payment_method, src.currency, src.amount,
                src.tax_amount, src.total_amount, src.items_count, src.updated_at,
                src.is_deleted, src.run_id, src.source_file, src.ingested_at
            );
        """

        self.con.execute("BEGIN TRANSACTION;")
        try:
            self.con.execute(merge_sql)
            self.con.execute("COMMIT;")
        except Exception:
            self.con.execute("ROLLBACK;")
            raise
        finally:
            self.con.execute(f"DROP TABLE IF EXISTS {staging_table};")

        return {
            "inserted": inserted_count,
            "updated": updated_count,
            "deleted": 0
        }
