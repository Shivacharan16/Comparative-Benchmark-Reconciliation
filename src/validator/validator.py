"""
Data validation, schema enforcement, business-rule verification,
referential integrity checking, intra-batch deduplication, and quarantine management.
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Tuple, Set, Optional
import pyarrow as pa
import pyarrow.parquet as pq

from config.settings import QUARANTINE_DIR
from src.schemas.order import OrderRawSchema, OrderProcessedSchema

logger = logging.getLogger(__name__)

class BatchValidator:
    """
    Validates ingested raw records, enforces schema constraints, checks business rules,
    resolves intra-batch duplicate keys, and routes failed records to quarantine.
    """

    def __init__(self, quarantine_dir: Path = QUARANTINE_DIR):
        self.quarantine_dir = quarantine_dir
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)

    def validate_batch(
        self,
        raw_items: List[Any],
        valid_customer_ids: Optional[Set[str]] = None,
        run_id: str = "run_default",
        source_file: str = "unknown_source"
    ) -> Tuple[List[OrderProcessedSchema], List[Dict[str, Any]]]:
        """
        Validates a list of raw records (dicts or raw JSON strings).
        Returns (valid_processed_orders, rejected_records_metadata).
        """
        valid_processed: List[OrderProcessedSchema] = []
        rejected: List[Dict[str, Any]] = []
        ingested_at = datetime.now(timezone.utc).isoformat()

        # Phase 1: Individual record schema & business rule validation
        preliminary_valid: List[OrderRawSchema] = []

        for item_idx, item in enumerate(raw_items):
            raw_payload = item
            # Handle unparseable strings or malformed JSON
            if isinstance(item, str):
                try:
                    parsed_dict = json.loads(item)
                    raw_payload = parsed_dict
                except Exception as ex:
                    rejected.append({
                        "record_index": item_idx,
                        "raw_payload": item,
                        "rejection_rule": "MALFORMED_JSON",
                        "rejection_reason": f"Failed to parse JSON string: {str(ex)}",
                        "rejected_at": datetime.now(timezone.utc).isoformat(),
                        "run_id": run_id,
                        "source_file": source_file
                    })
                    continue

            if not isinstance(raw_payload, dict):
                rejected.append({
                    "record_index": item_idx,
                    "raw_payload": str(raw_payload),
                    "rejection_rule": "INVALID_RECORD_TYPE",
                    "rejection_reason": f"Expected dict, got {type(raw_payload).__name__}",
                    "rejected_at": datetime.now(timezone.utc).isoformat(),
                    "run_id": run_id,
                    "source_file": source_file
                })
                continue

            # Remove test labels if present
            clean_dict = {k: v for k, v in raw_payload.items() if not k.startswith("_test_")}

            # Schema and business rule checks via Pydantic
            try:
                raw_order = OrderRawSchema(**clean_dict)
            except Exception as ex:
                rejected.append({
                    "record_index": item_idx,
                    "raw_payload": json.dumps(raw_payload),
                    "rejection_rule": "SCHEMA_OR_DOMAIN_CONSTRAINT",
                    "rejection_reason": str(ex),
                    "rejected_at": datetime.now(timezone.utc).isoformat(),
                    "run_id": run_id,
                    "source_file": source_file
                })
                continue

            # Referential integrity check
            if valid_customer_ids is not None and raw_order.customer_id not in valid_customer_ids:
                rejected.append({
                    "record_index": item_idx,
                    "raw_payload": json.dumps(raw_payload),
                    "rejection_rule": "REFERENTIAL_INTEGRITY_VIOLATION",
                    "rejection_reason": f"Customer ID '{raw_order.customer_id}' does not exist in dim_customers",
                    "rejected_at": datetime.now(timezone.utc).isoformat(),
                    "run_id": run_id,
                    "source_file": source_file
                })
                continue

            preliminary_valid.append(raw_order)

        # Phase 2: Intra-batch deduplication
        # If the same order_id appears multiple times within the batch,
        # sort by updated_at descending; the most recent event wins.
        # Older duplicates are flagged and quarantined to preserve auditability.
        grouped_by_id: Dict[str, List[OrderRawSchema]] = {}
        for order in preliminary_valid:
            grouped_by_id.setdefault(order.order_id, []).append(order)

        for order_id, group in grouped_by_id.items():
            if len(group) == 1:
                processed = OrderProcessedSchema.from_raw(
                    group[0], run_id=run_id, source_file=source_file, ingested_at=ingested_at
                )
                valid_processed.append(processed)
            else:
                # Multiple events for same key in one batch
                # Sort descending by updated_at
                sorted_group = sorted(group, key=lambda x: x.updated_at, reverse=True)
                winning_record = sorted_group[0]
                processed = OrderProcessedSchema.from_raw(
                    winning_record, run_id=run_id, source_file=source_file, ingested_at=ingested_at
                )
                valid_processed.append(processed)

                # Route shadowed older intra-batch duplicates to rejected / duplicate quarantine
                for shadowed in sorted_group[1:]:
                    rejected.append({
                        "record_index": -1,
                        "raw_payload": json.dumps(shadowed.model_dump()),
                        "rejection_rule": "INTRA_BATCH_SHADOWED_DUPLICATE",
                        "rejection_reason": f"Key {order_id} superseded by newer event with updated_at {winning_record.updated_at}",
                        "rejected_at": datetime.now(timezone.utc).isoformat(),
                        "run_id": run_id,
                        "source_file": source_file
                    })

        return valid_processed, rejected

    def quarantine_records(self, rejected_records: List[Dict[str, Any]], run_id: str) -> Optional[Path]:
        """
        Persists rejected records to an explicit quarantine sink (both JSONL and Parquet).
        Enables data stewards to audit, trace, and repair failed records.
        """
        if not rejected_records:
            return None

        # Write to JSONL
        jsonl_path = self.quarantine_dir / f"quarantine_{run_id}.jsonl"
        with open(jsonl_path, "w", encoding="utf-8") as f:
            for r in rejected_records:
                f.write(json.dumps(r) + "\n")

        # Write to Parquet for fast analytical querying
        parquet_path = self.quarantine_dir / f"quarantine_{run_id}.parquet"
        table = pa.Table.from_pylist(rejected_records)
        pq.write_table(table, parquet_path, compression="snappy")

        logger.warning(f"Quarantined {len(rejected_records)} records to {parquet_path}")
        return parquet_path
