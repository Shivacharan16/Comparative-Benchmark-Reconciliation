"""
Tests for Strategy 2: Incremental Merge Pipeline (UPSERT / CDC).
Tests inserts, updates, watermark protection against out-of-order writes, and idempotency.
"""
import pytest
from src.generator.data_generator import DatasetGenerator

def test_incremental_merge_insert_and_update(seeded_pipelines):
    pipe = seeded_pipelines["inc"]
    con = seeded_pipelines["conn"]

    initial_record = [{
        "order_id": "ORD-INC-001",
        "customer_id": seeded_pipelines["customers"][0]["customer_id"],
        "order_date": "2026-10-25",
        "order_timestamp": "2026-10-25T10:00:00Z",
        "order_status": "PENDING",
        "payment_method": "CREDIT_CARD",
        "currency": "USD",
        "amount": 100.0,
        "items_count": 1,
        "updated_at": "2026-10-25T10:00:00Z",
        "is_deleted": False
    }]

    # Ingest Initial
    res1 = pipe.run(initial_record, batch_id="b1")
    assert res1["status"] == "SUCCESS"
    assert pipe.get_target_count() == 1

    # Ingest newer update for existing key + new insert
    delta_batch = [
        # Newer update
        {
            "order_id": "ORD-INC-001",
            "customer_id": seeded_pipelines["customers"][0]["customer_id"],
            "order_date": "2026-10-25",
            "order_timestamp": "2026-10-25T10:00:00Z",
            "order_status": "DELIVERED",
            "payment_method": "CREDIT_CARD",
            "currency": "USD",
            "amount": 120.0,
            "items_count": 1,
            "updated_at": "2026-10-26T12:00:00Z",  # Newer timestamp
            "is_deleted": False
        },
        # New insert
        {
            "order_id": "ORD-INC-002",
            "customer_id": seeded_pipelines["customers"][0]["customer_id"],
            "order_date": "2026-10-26",
            "order_timestamp": "2026-10-26T10:00:00Z",
            "order_status": "PENDING",
            "payment_method": "CREDIT_CARD",
            "currency": "USD",
            "amount": 250.0,
            "items_count": 3,
            "updated_at": "2026-10-26T10:00:00Z",
            "is_deleted": False
        }
    ]

    res2 = pipe.run(delta_batch, batch_id="b2")
    assert res2["status"] == "SUCCESS"
    assert pipe.get_target_count() == 2

    # Check updated values
    row = con.execute(f"SELECT order_status, amount FROM {pipe.target_table} WHERE order_id = 'ORD-INC-001'").fetchone()
    assert row[0] == "DELIVERED"
    assert row[1] == 120.0

def test_incremental_watermark_protection_against_out_of_order_event(seeded_pipelines):
    """
    CRITICAL EDGE CASE:
    An out-of-order event arrives with a stale updated_at timestamp.
    The pipeline MUST NOT allow the stale event to overwrite fresher data.
    """
    pipe = seeded_pipelines["inc"]
    con = seeded_pipelines["conn"]

    fresh_order = [{
        "order_id": "ORD-OUT-OF-ORDER",
        "customer_id": seeded_pipelines["customers"][0]["customer_id"],
        "order_date": "2026-10-25",
        "order_timestamp": "2026-10-25T10:00:00Z",
        "order_status": "DELIVERED",
        "payment_method": "CREDIT_CARD",
        "currency": "USD",
        "amount": 500.0,
        "items_count": 2,
        "updated_at": "2026-10-26T18:00:00Z",  # Fresher event
        "is_deleted": False
    }]
    pipe.run(fresh_order, batch_id="b_fresh")

    # Stale out-of-order event arriving later
    stale_order = [{
        "order_id": "ORD-OUT-OF-ORDER",
        "customer_id": seeded_pipelines["customers"][0]["customer_id"],
        "order_date": "2026-10-25",
        "order_timestamp": "2026-10-25T10:00:00Z",
        "order_status": "PENDING",  # Stale state
        "payment_method": "CREDIT_CARD",
        "currency": "USD",
        "amount": 9999.0,           # Stale value
        "items_count": 2,
        "updated_at": "2026-10-25T11:00:00Z",  # Stale timestamp!
        "is_deleted": False
    }]
    pipe.run(stale_order, batch_id="b_stale")

    # Target table should still retain the fresh values!
    row = con.execute(f"SELECT order_status, amount FROM {pipe.target_table} WHERE order_id = 'ORD-OUT-OF-ORDER'").fetchone()
    assert row[0] == "DELIVERED"
    assert row[1] == 500.0  # NOT 9999.0!

def test_incremental_merge_idempotency(seeded_pipelines):
    pipe = seeded_pipelines["inc"]
    records = [{
        "order_id": f"ORD-IDEM-{i}",
        "customer_id": seeded_pipelines["customers"][0]["customer_id"],
        "order_date": "2026-10-25",
        "order_timestamp": "2026-10-25T10:00:00Z",
        "order_status": "PENDING",
        "payment_method": "CREDIT_CARD",
        "currency": "USD",
        "amount": 100.0,
        "items_count": 1,
        "updated_at": "2026-10-25T10:00:00Z",
        "is_deleted": False
    } for i in range(10)]

    pipe.run(records, batch_id="run_1")
    count_1 = pipe.get_target_count()

    # Rerun exact same batch
    pipe.run(records, batch_id="run_2_rerun")
    count_2 = pipe.get_target_count()

    assert count_1 == count_2 == 10
