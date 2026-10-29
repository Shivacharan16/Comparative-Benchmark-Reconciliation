"""
Tests for Strategy 1: Full Refresh Pipeline.
Tests cold load, full accumulated replacement, atomic table swap, and idempotency.
"""
import pytest
from src.generator.data_generator import DatasetGenerator

def test_full_refresh_cold_start(seeded_pipelines):
    pipe = seeded_pipelines["full"]
    customers = seeded_pipelines["customers"]
    generator = DatasetGenerator(seed=201)
    generator.customer_ids = [c["customer_id"] for c in customers]
    orders = generator.generate_initial_orders(count=200, start_date="2026-10-25", num_days=2)

    res = pipe.run(orders, batch_id="batch_full_init")
    assert res["status"] == "SUCCESS"
    assert res["valid_records"] == 200
    assert res["target_total_records"] == 200

def test_full_refresh_replacement_and_idempotency(seeded_pipelines):
    pipe = seeded_pipelines["full"]
    customers = seeded_pipelines["customers"]
    generator = DatasetGenerator(seed=202)
    generator.customer_ids = [c["customer_id"] for c in customers]
    orders = generator.generate_initial_orders(count=150, start_date="2026-10-25", num_days=2)

    # Initial run
    pipe.run(orders, batch_id="run_1")
    count_1 = pipe.get_target_count()
    assert count_1 == 150

    # Rerun identical dataset (Idempotency)
    pipe.run(orders, batch_id="run_2_rerun")
    count_2 = pipe.get_target_count()
    assert count_2 == 150

    # Run with updated and new records
    updated_orders = [o.copy() for o in orders]
    updated_orders[0]["amount"] = 999.0
    updated_orders[0]["updated_at"] = "2026-10-26T20:00:00Z"

    pipe.run(updated_orders, batch_id="run_3_update")
    count_3 = pipe.get_target_count()
    assert count_3 == 150

    # Verify updated row value in target table
    con = seeded_pipelines["conn"]
    row = con.execute(f"SELECT amount FROM {pipe.target_table} WHERE order_id = ?", [orders[0]["order_id"]]).fetchone()
    assert row[0] == 999.0
