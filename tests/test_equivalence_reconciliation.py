"""
Tests for Cross-Strategy Equivalence and Mathematical Reconciliation.
Proves that Full Refresh, Incremental Merge, and Partition Replacement produce
100% equivalent final states across complex insert/update/delete/out-of-order streams.
"""
import pytest
from src.generator.data_generator import DatasetGenerator
from src.pipelines.full_refresh import FullRefreshPipeline
from src.pipelines.incremental_merge import IncrementalMergePipeline
from src.pipelines.partition_replace import PartitionReplacementPipeline
from src.reconciliation.reconciler import ReconciliationEngine

def test_full_cross_strategy_mathematical_equivalence(db_conn):
    """
    Executes a multi-batch stream (initial + delta with inserts, updates, deletes, late arrivals, out-of-order)
    across all three loading strategies and asserts mathematical identity.
    """
    generator = DatasetGenerator(seed=401)
    customers = generator.generate_customers(count=100)

    # 1. Generate baseline data
    initial_orders = generator.generate_initial_orders(
        count=300, start_date="2026-10-25", num_days=3
    )

    # 2. Generate CDC delta
    delta_orders, metrics = generator.generate_incremental_orders(
        existing_orders=initial_orders,
        new_inserts_count=100,
        updates_ratio=0.25,
        soft_deletes_ratio=0.05,
        out_of_order_count=10,
        late_arriving_count=10
    )

    # Accumulated state for Full Refresh (with watermark awareness)
    accumulated_dict = {o["order_id"]: o for o in initial_orders}
    for d in delta_orders:
        curr = accumulated_dict.get(d["order_id"])
        if curr is None or d["updated_at"] >= curr["updated_at"]:
            accumulated_dict[d["order_id"]] = d
    accumulated_orders = list(accumulated_dict.values())

    # Initialize pipelines on isolated test database
    pipe_full = FullRefreshPipeline(db_conn, "orders_full_refresh")
    pipe_inc = IncrementalMergePipeline(db_conn, "orders_incremental_merge")
    pipe_part = PartitionReplacementPipeline(db_conn, "orders_partition_replace")

    pipe_full.load_customers(customers)
    pipe_inc.load_customers(customers)
    pipe_part.load_customers(customers)

    # Ingest Batch 1 (Baseline)
    pipe_full.run(initial_orders, batch_id="b1")
    pipe_inc.run(initial_orders, batch_id="b1")
    pipe_part.run(initial_orders, batch_id="b1")

    # Ingest Batch 2 (Incremental CDC Delta)
    pipe_full.run(accumulated_orders, batch_id="b2")
    pipe_inc.run(delta_orders, batch_id="b2")
    pipe_part.run(delta_orders, batch_id="b2")

    # Run Reconciliation Engine
    reconciler = ReconciliationEngine(db_conn)

    # 1. Row counts check
    counts = reconciler.check_row_counts()
    assert counts["Full Refresh"] == counts["Incremental Merge"] == counts["Partition Replace"] == 400

    # 2. Key set equivalence check
    key_check = reconciler.check_key_set_equivalence()
    assert key_check["is_identical"] is True
    assert len(key_check["diff_full_minus_inc"]) == 0
    assert len(key_check["diff_inc_minus_full"]) == 0
    assert len(key_check["diff_full_minus_part"]) == 0

    # 3. Aggregate totals check
    aggs = reconciler.check_aggregates()
    full_agg = aggs["Full Refresh"]
    inc_agg = aggs["Incremental Merge"]
    part_agg = aggs["Partition Replace"]

    assert abs(full_agg["sum_amount"] - inc_agg["sum_amount"]) < 0.01
    assert abs(inc_agg["sum_amount"] - part_agg["sum_amount"]) < 0.01

    assert abs(full_agg["sum_tax"] - inc_agg["sum_tax"]) < 0.01
    assert abs(inc_agg["sum_tax"] - part_agg["sum_tax"]) < 0.01

    assert abs(full_agg["sum_total_amount"] - inc_agg["sum_total_amount"]) < 0.01
    assert abs(inc_agg["sum_total_amount"] - part_agg["sum_total_amount"]) < 0.01

    assert full_agg["sum_items"] == inc_agg["sum_items"] == part_agg["sum_items"]
    assert full_agg["distinct_customers"] == inc_agg["distinct_customers"] == part_agg["distinct_customers"]

    # 4. Partition level check
    part_reconciliation = reconciler.check_partition_level_equivalence()
    assert all(p["is_matched"] for p in part_reconciliation)

    # 5. Cell-level deterministic MD5 Checksum
    checksums = reconciler.check_deterministic_checksums()
    assert checksums["Full Refresh"] == checksums["Incremental Merge"] == checksums["Partition Replace"]
    assert len(checksums["Full Refresh"]) == 32  # Valid MD5 hex string
