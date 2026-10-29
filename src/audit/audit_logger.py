"""
Audit logging, operational metrics tracking, and run-lineage recorder.
Stores execution metadata in the analytical database and provides tabular reporting.
"""
import uuid
import duckdb
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from tabulate import tabulate

from config.settings import TABLE_AUDIT_LOG

AUDIT_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_AUDIT_LOG} (
    run_id VARCHAR PRIMARY KEY,
    pipeline_strategy VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    duration_seconds DOUBLE NOT NULL,
    input_records INTEGER NOT NULL,
    valid_records INTEGER NOT NULL,
    rejected_records INTEGER NOT NULL,
    inserted_records INTEGER NOT NULL,
    updated_records INTEGER NOT NULL,
    deleted_records INTEGER NOT NULL,
    target_total_records INTEGER NOT NULL,
    error_message VARCHAR
);
"""

class AuditLogger:
    """
    Tracks and persists operational pipeline run metrics.
    Ensures end-to-end traceability across runs and failures.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con
        self._init_table()

    def _init_table(self):
        self.con.execute(AUDIT_TABLE_DDL)

    def log_run(
        self,
        run_id: str,
        pipeline_strategy: str,
        batch_id: str,
        status: str,
        start_time: datetime,
        end_time: datetime,
        input_records: int,
        valid_records: int,
        rejected_records: int,
        inserted_records: int,
        updated_records: int,
        deleted_records: int,
        target_total_records: int,
        error_message: Optional[str] = None
    ) -> Dict[str, Any]:
        """Inserts an audit record into pipeline_audit_log."""
        duration = round((end_time - start_time).total_seconds(), 4)

        query = f"""
        INSERT INTO {TABLE_AUDIT_LOG} (
            run_id, pipeline_strategy, batch_id, status, start_time, end_time,
            duration_seconds, input_records, valid_records, rejected_records,
            inserted_records, updated_records, deleted_records, target_total_records, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        self.con.execute(query, [
            run_id,
            pipeline_strategy,
            batch_id,
            status,
            start_time.isoformat(),
            end_time.isoformat(),
            duration,
            input_records,
            valid_records,
            rejected_records,
            inserted_records,
            updated_records,
            deleted_records,
            target_total_records,
            error_message or ""
        ])

        return {
            "run_id": run_id,
            "pipeline_strategy": pipeline_strategy,
            "batch_id": batch_id,
            "status": status,
            "duration_seconds": duration,
            "input_records": input_records,
            "valid_records": valid_records,
            "rejected_records": rejected_records,
            "target_total_records": target_total_records
        }

    def get_run_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Retrieves historical audit entries."""
        cursor = self.con.execute(f"SELECT * FROM {TABLE_AUDIT_LOG} ORDER BY start_time DESC LIMIT ?", [limit])
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def print_audit_summary(self, limit: int = 10) -> str:
        """Returns a formatted CLI table of recent pipeline runs."""
        history = self.get_run_history(limit=limit)
        if not history:
            return "No audit records found."

        headers = ["Run ID", "Strategy", "Status", "Duration (s)", "Input", "Valid", "Quarantined", "Target Count"]
        table_data = []
        for h in history:
            table_data.append([
                h["run_id"][:12] + "...",
                h["pipeline_strategy"],
                h["status"],
                f"{h['duration_seconds']:.3f}",
                h["input_records"],
                h["valid_records"],
                h["rejected_records"],
                h["target_total_records"]
            ])

        return tabulate(table_data, headers=headers, tablefmt="fancy_grid")
