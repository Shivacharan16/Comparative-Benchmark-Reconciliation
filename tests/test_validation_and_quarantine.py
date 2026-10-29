"""
Tests for schema validation, business-rule enforcement,
foreign key verification, intra-batch deduplication, and quarantine routing.
"""
import pytest
import json
from pathlib import Path
from src.validator.validator import BatchValidator

def test_happy_path_validation(tmp_path):
    validator = BatchValidator(quarantine_dir=tmp_path)
    cust_ids = {"CUST-0001"}
    valid_batch = [
        {
            "order_id": "ORD-101",
            "customer_id": "CUST-0001",
            "order_date": "2026-10-25",
            "order_timestamp": "2026-10-25T12:00:00Z",
            "order_status": "PENDING",
            "payment_method": "CREDIT_CARD",
            "currency": "USD",
            "amount": 100.0,
            "items_count": 2,
            "updated_at": "2026-10-25T12:00:00Z",
            "is_deleted": False
        }
    ]

    valid, rejected = validator.validate_batch(valid_batch, valid_customer_ids=cust_ids)
    assert len(valid) == 1
    assert len(rejected) == 0
    assert valid[0].order_id == "ORD-101"
    assert valid[0].tax_amount == 18.0
    assert valid[0].total_amount == 118.0

def test_empty_input_batch(tmp_path):
    validator = BatchValidator(quarantine_dir=tmp_path)
    valid, rejected = validator.validate_batch([], valid_customer_ids={"CUST-0001"})
    assert len(valid) == 0
    assert len(rejected) == 0

def test_malformed_json_records(tmp_path):
    validator = BatchValidator(quarantine_dir=tmp_path)
    bad_items = [
        "{UNPARSEABLE_CORRUPTED_JSON_LINE",
        12345,  # Non-dict / non-string type
    ]
    valid, rejected = validator.validate_batch(bad_items, valid_customer_ids={"CUST-0001"})
    assert len(valid) == 0
    assert len(rejected) == 2
    assert rejected[0]["rejection_rule"] == "MALFORMED_JSON"
    assert rejected[1]["rejection_rule"] == "INVALID_RECORD_TYPE"

def test_missing_required_fields_and_nulls(tmp_path):
    validator = BatchValidator(quarantine_dir=tmp_path)
    cust_ids = {"CUST-0001"}
    records = [
        # Missing order_id
        {
            "customer_id": "CUST-0001",
            "order_date": "2026-10-25",
            "order_timestamp": "2026-10-25T12:00:00Z",
            "order_status": "PENDING",
            "payment_method": "CREDIT_CARD",
            "currency": "USD",
            "amount": 50.0,
            "items_count": 1,
            "updated_at": "2026-10-25T12:00:00Z"
        },
        # Null customer_id
        {
            "order_id": "ORD-202",
            "customer_id": None,
            "order_date": "2026-10-25",
            "order_timestamp": "2026-10-25T12:00:00Z",
            "order_status": "PENDING",
            "payment_method": "CREDIT_CARD",
            "currency": "USD",
            "amount": 50.0,
            "items_count": 1,
            "updated_at": "2026-10-25T12:00:00Z"
        }
    ]

    valid, rejected = validator.validate_batch(records, valid_customer_ids=cust_ids)
    assert len(valid) == 0
    assert len(rejected) == 2
    assert rejected[0]["rejection_rule"] == "SCHEMA_OR_DOMAIN_CONSTRAINT"
    assert rejected[1]["rejection_rule"] == "SCHEMA_OR_DOMAIN_CONSTRAINT"

def test_domain_edge_case_negative_and_zero_amount(tmp_path):
    validator = BatchValidator(quarantine_dir=tmp_path)
    cust_ids = {"CUST-0001"}
    records = [
        # Negative amount
        {
            "order_id": "ORD-NEG",
            "customer_id": "CUST-0001",
            "order_date": "2026-10-25",
            "order_timestamp": "2026-10-25T12:00:00Z",
            "order_status": "PENDING",
            "payment_method": "CREDIT_CARD",
            "currency": "USD",
            "amount": -50.0,
            "items_count": 1,
            "updated_at": "2026-10-25T12:00:00Z"
        },
        # Zero amount
        {
            "order_id": "ORD-ZERO",
            "customer_id": "CUST-0001",
            "order_date": "2026-10-25",
            "order_timestamp": "2026-10-25T12:00:00Z",
            "order_status": "PENDING",
            "payment_method": "CREDIT_CARD",
            "currency": "USD",
            "amount": 0.0,
            "items_count": 1,
            "updated_at": "2026-10-25T12:00:00Z"
        }
    ]

    valid, rejected = validator.validate_batch(records, valid_customer_ids=cust_ids)
    assert len(valid) == 0
    assert len(rejected) == 2
    assert "Amount must be strictly positive" in rejected[0]["rejection_reason"]
    assert "Amount must be strictly positive" in rejected[1]["rejection_reason"]

def test_domain_edge_case_items_count(tmp_path):
    validator = BatchValidator(quarantine_dir=tmp_path)
    record = {
        "order_id": "ORD-ITEMS",
        "customer_id": "CUST-0001",
        "order_date": "2026-10-25",
        "order_timestamp": "2026-10-25T12:00:00Z",
        "order_status": "PENDING",
        "payment_method": "CREDIT_CARD",
        "currency": "USD",
        "amount": 100.0,
        "items_count": 0,  # Invalid
        "updated_at": "2026-10-25T12:00:00Z"
    }
    valid, rejected = validator.validate_batch([record], valid_customer_ids={"CUST-0001"})
    assert len(valid) == 0
    assert len(rejected) == 1
    assert "items_count" in rejected[0]["rejection_reason"]

def test_foreign_key_referential_integrity(tmp_path):
    validator = BatchValidator(quarantine_dir=tmp_path)
    cust_ids = {"CUST-0001", "CUST-0002"}
    records = [
        {
            "order_id": "ORD-GOOD-FK",
            "customer_id": "CUST-0001",
            "order_date": "2026-10-25",
            "order_timestamp": "2026-10-25T12:00:00Z",
            "order_status": "PENDING",
            "payment_method": "CREDIT_CARD",
            "currency": "USD",
            "amount": 100.0,
            "items_count": 1,
            "updated_at": "2026-10-25T12:00:00Z"
        },
        {
            "order_id": "ORD-BAD-FK",
            "customer_id": "CUST-UNKNOWN-9999",
            "order_date": "2026-10-25",
            "order_timestamp": "2026-10-25T12:00:00Z",
            "order_status": "PENDING",
            "payment_method": "CREDIT_CARD",
            "currency": "USD",
            "amount": 100.0,
            "items_count": 1,
            "updated_at": "2026-10-25T12:00:00Z"
        }
    ]

    valid, rejected = validator.validate_batch(records, valid_customer_ids=cust_ids)
    assert len(valid) == 1
    assert len(rejected) == 1
    assert valid[0].order_id == "ORD-GOOD-FK"
    assert rejected[0]["rejection_rule"] == "REFERENTIAL_INTEGRITY_VIOLATION"

def test_intra_batch_deduplication(tmp_path):
    validator = BatchValidator(quarantine_dir=tmp_path)
    cust_ids = {"CUST-0001"}
    # Two events for same order_id in the same batch
    records = [
        {
            "order_id": "ORD-DUP",
            "customer_id": "CUST-0001",
            "order_date": "2026-10-25",
            "order_timestamp": "2026-10-25T10:00:00Z",
            "order_status": "PENDING",
            "payment_method": "CREDIT_CARD",
            "currency": "USD",
            "amount": 100.0,
            "items_count": 1,
            "updated_at": "2026-10-25T10:00:00Z"
        },
        {
            "order_id": "ORD-DUP",
            "customer_id": "CUST-0001",
            "order_date": "2026-10-25",
            "order_timestamp": "2026-10-25T10:00:00Z",
            "order_status": "SHIPPED",
            "payment_method": "CREDIT_CARD",
            "currency": "USD",
            "amount": 150.0,
            "items_count": 1,
            "updated_at": "2026-10-25T14:00:00Z"  # Newer event
        }
    ]

    valid, rejected = validator.validate_batch(records, valid_customer_ids=cust_ids)
    assert len(valid) == 1
    assert len(rejected) == 1
    # Latest event won
    assert valid[0].order_status == "SHIPPED"
    assert valid[0].amount == 150.0
    # Shadowed older event was routed to quarantine
    assert rejected[0]["rejection_rule"] == "INTRA_BATCH_SHADOWED_DUPLICATE"

def test_quarantine_sink_persistence(tmp_path):
    validator = BatchValidator(quarantine_dir=tmp_path)
    bad_record = {"invalid": "data"}
    _, rejected = validator.validate_batch([bad_record], valid_customer_ids={"CUST-1"})

    pq_path = validator.quarantine_records(rejected, run_id="test_run_123")
    assert pq_path is not None
    assert pq_path.exists()

    # Read back quarantine parquet file
    import duckdb
    con = duckdb.connect(":memory:")
    quarantined_rows = con.execute(f"SELECT * FROM read_parquet('{str(pq_path).replace('\\', '/')}')").fetchall()
    assert len(quarantined_rows) == 1
    con.close()
