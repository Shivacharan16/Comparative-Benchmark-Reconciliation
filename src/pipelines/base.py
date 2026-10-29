"""
Abstract Base Class for Data Ingestion Pipelines.
Enforces common contract: schema creation, validation, quarantine, audit logging, and idempotency.
"""
from abc import ABC, abstractmethod
import uuid
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Set
import duckdb

from config.settings import TABLE_CUSTOMERS, TABLE_AUDIT_LOG
from src.schemas.customer import CUSTOMER_TABLE_DDL
from src.schemas.order import OrderProcessedSchema
from src.validator.validator import BatchValidator
from src.audit.audit_logger import AuditLogger

logger = logging.getLogger(__name__)

class BasePipeline(ABC):
    """
    Abstract pipeline defining the lifecycle of data ingestion:
    1. Setup database / tables
    2. Read & validate batch
    3. Route rejects to quarantine
    4. Execute strategy-specific loading (atomic swap, merge, partition replace)
    5. Record operational metrics and audit history
    """

    def __init__(self, con: duckdb.DuckDBPyConnection, target_table: str):
        self.con = con
        self.target_table = target_table
        self.validator = BatchValidator()
        self.audit_logger = AuditLogger(con)
        self._init_base_tables()

    def _init_base_tables(self):
        """Initializes customer dimension and audit tables."""
        self.con.execute(CUSTOMER_TABLE_DDL)

    def load_customers(self, customers: List[Dict[str, Any]]) -> int:
        """Loads or refreshes reference customers dimension table for FK checks."""
        if not customers:
            return 0

        # Truncate and reload dimension
        self.con.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_CUSTOMERS} AS SELECT * FROM dim_customers WHERE 1=0;")
        self.con.execute(f"DELETE FROM {TABLE_CUSTOMERS};")

        # Insert via parameterized statement or duckdb DataFrame/Arrow
        for c in customers:
            self.con.execute(f"""
            INSERT INTO {TABLE_CUSTOMERS} (customer_id, customer_name, customer_email, customer_tier, country_code, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """, [c["customer_id"], c["customer_name"], c["customer_email"], c["customer_tier"], c["country_code"], c["created_at"]])

        count = self.con.execute(f"SELECT COUNT(*) FROM {TABLE_CUSTOMERS}").fetchone()[0]
        return count

    def get_valid_customer_ids(self) -> Set[str]:
        """Fetches known customer IDs for referential integrity checking."""
        cursor = self.con.execute(f"SELECT customer_id FROM {TABLE_CUSTOMERS}")
        return {row[0] for row in cursor.fetchall()}

    def get_target_count(self) -> int:
        """Returns current record count in the target table."""
        try:
            return self.con.execute(f"SELECT COUNT(*) FROM {self.target_table}").fetchone()[0]
        except Exception:
            return 0

    @abstractmethod
    def execute_load(
        self,
        valid_records: List[OrderProcessedSchema],
        run_id: str,
        source_file: str
    ) -> Dict[str, int]:
        """Strategy-specific load execution. Returns {'inserted': x, 'updated': y, 'deleted': z}."""
        pass

    def run(
        self,
        raw_items: List[Any],
        batch_id: str = "batch_001",
        source_file: str = "inline_stream"
    ) -> Dict[str, Any]:
        """
        Executes end-to-end ingestion lifecycle with audit recording and quarantine routing.
        """
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        start_time = datetime.now(timezone.utc)
        strategy_name = self.__class__.__name__

        logger.info(f"[{strategy_name}] Starting run {run_id} for batch {batch_id} ({len(raw_items)} raw items)")

        try:
            # 1. Fetch reference keys for referential integrity
            valid_cust_ids = self.get_valid_customer_ids()

            # 2. Schema validation, business-rule checks, intra-batch deduplication
            valid_orders, rejected_records = self.validator.validate_batch(
                raw_items=raw_items,
                valid_customer_ids=valid_cust_ids,
                run_id=run_id,
                source_file=source_file
            )

            # 3. Route rejected records to quarantine
            self.validator.quarantine_records(rejected_records, run_id=run_id)

            # 4. Strategy-specific ingestion
            load_metrics = self.execute_load(
                valid_records=valid_orders,
                run_id=run_id,
                source_file=source_file
            )

            end_time = datetime.now(timezone.utc)
            final_target_count = self.get_target_count()

            # 5. Record audit metrics
            audit_result = self.audit_logger.log_run(
                run_id=run_id,
                pipeline_strategy=strategy_name,
                batch_id=batch_id,
                status="SUCCESS",
                start_time=start_time,
                end_time=end_time,
                input_records=len(raw_items),
                valid_records=len(valid_orders),
                rejected_records=len(rejected_records),
                inserted_records=load_metrics.get("inserted", 0),
                updated_records=load_metrics.get("updated", 0),
                deleted_records=load_metrics.get("deleted", 0),
                target_total_records=final_target_count,
                error_message=None
            )

            audit_result["load_metrics"] = load_metrics
            return audit_result

        except Exception as ex:
            end_time = datetime.now(timezone.utc)
            logger.error(f"[{strategy_name}] Run {run_id} failed: {str(ex)}", exc_info=True)
            self.audit_logger.log_run(
                run_id=run_id,
                pipeline_strategy=strategy_name,
                batch_id=batch_id,
                status="FAILED",
                start_time=start_time,
                end_time=end_time,
                input_records=len(raw_items),
                valid_records=0,
                rejected_records=0,
                inserted_records=0,
                updated_records=0,
                deleted_records=0,
                target_total_records=self.get_target_count(),
                error_message=str(ex)
            )
            raise
