"""
Tests for Strategy 3: Partition Replacement Pipeline.
Tests partition-level isolation, dynamic partition overwrite, consolidation, and idempotency.
"""
import pytest
from src.generator.data_generator import DatasetGenerator

def test_partition_replace_isolation(seeded_pipelines):
    pipe = seeded_pipelines["part"]
    con = seeded_pipelines["conn"]
    cust_id = seeded_pipelines["customers"][0]["customer_id"]

    # Initial data across 2 partitions: 2026-10-25 and 2026-10-26
    batch1 = [
        {
            "order_id": "ORD-P1",
            "customer_id": cust_id,
            "order_date": "2026-10-25",
            "order_timestamp": "2026-10-25T10:00:00Z",
            "order_status": "PENDING",
            "payment_method": "CREDIT_CARD",
            "currency": "USD",
            "amount": 100.0,
            "items_count": 1,
            "updated_at": "2026-10-25T10:00:00Z",
            "is_deleted": False
        },
        {
            "order_id": "ORD-P2",
            "customer_id": cust_id,
            "order_date": "2026-10-26",
            "order_timestamp": "2026-10-26T10:00:00Z",
            "order_status": "PENDING",
            "payment_method": "CREDIT_CARD",
            "currency": "USD",
            "amount": 200.0,
            "items_count": 2,
            "updated_at": "2026-10-26T10:00:00Z",
            "is_deleted": False
        }
    ]
    pipe.run(batch1, batch_id="init_part")
    assert pipe.get_target_count() == 2

    # Now ingest a delta touching ONLY partition 2026-10-26
    batch2 = [
        # Update to existing ORD-P2
        {
            "order_id": "ORD-P2",
            "customer_id": cust_id,
            "order_date": "2026-10-26",
            "order_timestamp": "2026-10-26T10:00:00Z",
            "order_status": "CONFIRMED",
            "payment_method": "CREDIT_CARD",
            "currency": "USD",
            "amount": 220.0,
            "items_count": 2,
            "updated_at": "2026-10-26T15:00:00Z",
            "is_deleted": False
        },
        # New order in partition 2026-10-26
        {
            "order_id": "ORD-P3",
            "customer_id": cust_id,
            "order_date": "2026-10-26",
            "order_timestamp": "2026-10-26T11:00:00Z",
            "order_status": "PENDING",
            "payment_method": "CREDIT_CARD",
            "currency": "USD",
            "amount": 300.0,
            "items_count": 3,
            "updated_at": "2026-10-26T11:00:00Z",
            "is_deleted": False
        }
    ]
    pipe.run(batch2, batch_id="delta_part")

    # Target table should have 3 rows total
    assert pipe.get_target_count() == 3

    # Verify partition 2026-10-25 was untouched
    row_p1 = con.execute(f"SELECT amount, order_status FROM {pipe.target_table} WHERE order_id = 'ORD-P1'").fetchone()
    assert row_p1[0] == 100.0
    assert row_p1[1] == "PENDING"

    # Verify partition 2026-10-26 has updated and new orders
    row_p2 = con.execute(f"SELECT amount, order_status FROM {pipe.target_table} WHERE order_id = 'ORD-P2'").fetchone()
    assert row_p2[0] == 220.0
    assert row_p2[1] == "CONFIRMED"

    row_p3 = con.execute(f"SELECT amount, order_status FROM {pipe.target_table} WHERE order_id = 'ORD-P3'").fetchone()
    assert row_p3[0] == 300.0

def test_partition_replace_idempotency(seeded_pipelines):
    pipe = seeded_pipelines["part"]
    cust_id = seeded_pipelines["customers"][0]["customer_id"]

    batch = [{
        "order_id": f"ORD-PART-IDEM-{i}",
        "customer_id": cust_id,
        "order_date": "2026-10-27",
        "order_timestamp": "2026-10-27T10:00:00Z",
        "order_status": "PENDING",
        "payment_method": "CREDIT_CARD",
        "currency": "USD",
        "amount": 150.0,
        "items_count": 2,
        "updated_at": "2026-10-27T10:00:00Z",
        "is_deleted": False
    } for i in range(5)]

    pipe.run(batch, batch_id="run_1")
    count_1 = pipe.get_target_count()

    pipe.run(batch, batch_id="run_2_rerun")
    count_2 = pipe.get_target_count()

    assert count_1 == count_2 == 5
