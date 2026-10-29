"""
Tests for pipeline idempotency, retry safety, and failure recovery / transaction rollback.
"""
import pytest
from unittest.mock import patch
from src.generator.data_generator import DatasetGenerator

def test_multi_strategy_idempotency_on_reruns(seeded_pipelines):
    """
    Rerunning the identical batch 3 times across all 3 strategies must:
    - Never produce duplicate rows
    - Keep identical row counts
    - Keep identical total amount sums
    """
    customers = seeded_pipelines["customers"]
    generator = DatasetGenerator(seed=301)
    generator.customer_ids = [c["customer_id"] for c in customers]
    orders = generator.generate_initial_orders(count=50, start_date="2026-10-25", num_days=2)
    con = seeded_pipelines["conn"]

    for name, pipe in [("full", seeded_pipelines["full"]), ("inc", seeded_pipelines["inc"]), ("part", seeded_pipelines["part"])]:
        # Run 1
        pipe.run(orders, batch_id="b1")
        cnt1 = pipe.get_target_count()
        sum1 = con.execute(f"SELECT SUM(total_amount) FROM {pipe.target_table}").fetchone()[0]

        # Run 2 (Rerun)
        pipe.run(orders, batch_id="b1_rerun_1")
        cnt2 = pipe.get_target_count()
        sum2 = con.execute(f"SELECT SUM(total_amount) FROM {pipe.target_table}").fetchone()[0]

        # Run 3 (Rerun)
        pipe.run(orders, batch_id="b1_rerun_2")
        cnt3 = pipe.get_target_count()
        sum3 = con.execute(f"SELECT SUM(total_amount) FROM {pipe.target_table}").fetchone()[0]

        assert cnt1 == cnt2 == cnt3 == 50, f"Idempotency violated for {name}: counts ({cnt1}, {cnt2}, {cnt3})"
        assert abs(sum1 - sum2) < 0.001 and abs(sum2 - sum3) < 0.001, f"Sum changed on rerun for {name}"

def test_atomic_failure_rollback(seeded_pipelines):
    """
    Tests that if a database failure or error occurs during load execution,
    the pipeline rolls back and leaves the prior committed target state untouched.
    """
    pipe = seeded_pipelines["inc"]
    cust_id = seeded_pipelines["customers"][0]["customer_id"]

    # Initial successful batch
    initial_batch = [{
        "order_id": "ORD-SAFE-1",
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
    }]
    pipe.run(initial_batch, batch_id="init_safe")
    assert pipe.get_target_count() == 1

    # Simulate an unexpected database failure during execute_load
    with patch.object(pipe, "execute_load", side_effect=RuntimeError("Simulated Disk Full Failure")):
        with pytest.raises(RuntimeError):
            pipe.run([{
                "order_id": "ORD-FAILING-2",
                "customer_id": cust_id,
                "order_date": "2026-10-25",
                "order_timestamp": "2026-10-25T11:00:00Z",
                "order_status": "PENDING",
                "payment_method": "CREDIT_CARD",
                "currency": "USD",
                "amount": 200.0,
                "items_count": 2,
                "updated_at": "2026-10-25T11:00:00Z",
                "is_deleted": False
            }], batch_id="failing_batch")

    # Target table should still have exactly the 1 pre-existing row
    assert pipe.get_target_count() == 1
