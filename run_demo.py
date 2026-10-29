"""
End-to-End Demonstration Script for Ingestion Strategy Comparison.
Executes baseline ingestion, incremental updates, edge-case quarantine,
cross-strategy reconciliation, and performance benchmarks.
"""
import sys
import os

# Set UTF-8 encoding for standard outputs on Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import duckdb
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console(force_terminal=True, legacy_windows=False)

from config.settings import (
    DATABASE_PATH,
    RAW_DATA_DIR,
    QUARANTINE_DIR,
    REPORTS_DIR,
    TABLE_FULL_REFRESH,
    TABLE_INCREMENTAL_MERGE,
    TABLE_PARTITION_REPLACE
)
from src.generator.data_generator import DatasetGenerator
from src.pipelines.full_refresh import FullRefreshPipeline
from src.pipelines.incremental_merge import IncrementalMergePipeline
from src.pipelines.partition_replace import PartitionReplacementPipeline
from src.reconciliation.reconciler import ReconciliationEngine
from src.benchmark.benchmark_runner import BenchmarkRunner
from src.audit.audit_logger import AuditLogger

console = Console()

def run_end_to_end_demo():
    console.print(Panel.fit(
        "[bold cyan]Data Engineering Pipeline Benchmark[/bold cyan]\n"
        "[dim]Comparing Full Refresh vs Incremental Merge vs Partition Replacement[/dim]",
        border_style="cyan"
    ))

    # 1. Initialize persistent warehouse connection
    con = duckdb.connect(str(DATABASE_PATH))

    # 2. Data Generation
    console.print("\n[bold yellow]Step 1: Generating Synthetic Data (Dimension, Baseline, Incremental, Edge-Cases)...[/bold yellow]")
    generator = DatasetGenerator(seed=42)

    customers = generator.generate_customers(count=300, save_path=RAW_DATA_DIR / "customers.jsonl")
    initial_orders = generator.generate_initial_orders(
        count=1000, start_date="2026-10-25", num_days=3, save_path=RAW_DATA_DIR / "initial_orders.jsonl"
    )
    delta_orders, delta_metrics = generator.generate_incremental_orders(
        existing_orders=initial_orders,
        new_inserts_count=200,
        updates_ratio=0.20,
        soft_deletes_ratio=0.03,
        out_of_order_count=10,
        late_arriving_count=15,
        save_path=RAW_DATA_DIR / "incremental_orders.jsonl"
    )
    edge_cases = generator.generate_edge_case_batch(save_path=RAW_DATA_DIR / "edge_case_orders.jsonl")

    console.print(f"  [OK] Generated [green]{len(customers)}[/green] customers")
    console.print(f"  [OK] Generated [green]{len(initial_orders)}[/green] baseline orders across 3 date partitions")
    console.print(f"  [OK] Generated [green]{len(delta_orders)}[/green] CDC delta records:")
    for k, v in delta_metrics.items():
        console.print(f"      - {k}: {v}")
    console.print(f"  [OK] Generated [green]{len(edge_cases)}[/green] deliberate edge-case records (malformed JSON, FK violations, duplicates, negative amounts)")

    # 3. Instantiate pipelines
    pipe_full = FullRefreshPipeline(con, TABLE_FULL_REFRESH)
    pipe_inc = IncrementalMergePipeline(con, TABLE_INCREMENTAL_MERGE)
    pipe_part = PartitionReplacementPipeline(con, TABLE_PARTITION_REPLACE)

    # Populate customer dimension
    pipe_full.load_customers(customers)

    # 4. Phase 1: Baseline Load
    console.print("\n[bold yellow]Step 2: Executing Baseline Batch Ingestion (Cold Start)...[/bold yellow]")
    res_full1 = pipe_full.run(initial_orders, batch_id="batch_001_baseline", source_file="initial_orders.jsonl")
    res_inc1 = pipe_inc.run(initial_orders, batch_id="batch_001_baseline", source_file="initial_orders.jsonl")
    res_part1 = pipe_part.run(initial_orders, batch_id="batch_001_baseline", source_file="initial_orders.jsonl")

    table1 = Table(title="Baseline Ingestion Metrics")
    table1.add_column("Strategy", style="cyan")
    table1.add_column("Status", style="green")
    table1.add_column("Valid Input", justify="right")
    table1.add_column("Target Count", justify="right")
    table1.add_column("Duration (s)", justify="right")

    for r in [res_full1, res_inc1, res_part1]:
        table1.add_row(r["pipeline_strategy"], r["status"], str(r["valid_records"]), str(r["target_total_records"]), f"{r['duration_seconds']:.4f}")
    console.print(table1)

    # 5. Phase 2: Incremental Load + Edge Case Handling
    console.print("\n[bold yellow]Step 3: Ingesting Incremental CDC Delta + Edge Cases...[/bold yellow]")
    combined_delta = delta_orders + edge_cases

    # For Full Refresh: accumulated state representing source database
    # In Full Refresh, we merge delta into source snapshot respecting event watermark
    accumulated_dict = {o["order_id"]: o for o in initial_orders}
    for d in delta_orders:
        curr = accumulated_dict.get(d["order_id"])
        if curr is None or d["updated_at"] >= curr["updated_at"]:
            accumulated_dict[d["order_id"]] = d
    accumulated_source_orders = list(accumulated_dict.values()) + edge_cases

    res_full2 = pipe_full.run(accumulated_source_orders, batch_id="batch_002_incremental", source_file="accumulated.jsonl")
    res_inc2 = pipe_inc.run(combined_delta, batch_id="batch_002_incremental", source_file="incremental_orders.jsonl")
    res_part2 = pipe_part.run(combined_delta, batch_id="batch_002_incremental", source_file="incremental_orders.jsonl")

    table2 = Table(title="Incremental Load & Quarantine Metrics")
    table2.add_column("Strategy", style="cyan")
    table2.add_column("Input Rows", justify="right")
    table2.add_column("Valid Rows", justify="right")
    table2.add_column("Quarantined", justify="right", style="red")
    table2.add_column("Target Final Count", justify="right", style="bold")
    table2.add_column("Duration (s)", justify="right")

    for r in [res_full2, res_inc2, res_part2]:
        table2.add_row(
            r["pipeline_strategy"],
            str(r["input_records"]),
            str(r["valid_records"]),
            str(r["rejected_records"]),
            str(r["target_total_records"]),
            f"{r['duration_seconds']:.4f}"
        )
    console.print(table2)

    # 6. Inspect Quarantine
    console.print("\n[bold yellow]Step 4: Inspecting Quarantined Edge Cases...[/bold yellow]")
    quarantine_files = list(QUARANTINE_DIR.glob("*.parquet"))
    if quarantine_files:
        latest_q = sorted(quarantine_files)[-1]
        q_con = duckdb.connect(":memory:")
        q_summary = q_con.execute(f"""
            SELECT rejection_rule, COUNT(*) as count, MIN(rejection_reason) as sample_reason
            FROM read_parquet('{latest_q}')
            GROUP BY rejection_rule
        """).fetchall()

        q_table = Table(title=f"Quarantine Sample: {latest_q.name}")
        q_table.add_column("Rejection Rule", style="magenta")
        q_table.add_column("Count", justify="right")
        q_table.add_column("Sample Reason", style="dim")

        for row in q_summary:
            q_table.add_row(row[0], str(row[1]), str(row[2])[:70] + "...")
        console.print(q_table)

    # 7. Cross-Strategy Reconciliation
    console.print("\n[bold yellow]Step 5: Verifying Cross-Strategy Mathematical Equivalence...[/bold yellow]")
    reconciler = ReconciliationEngine(con)
    report_path = REPORTS_DIR / "demo_reconciliation_report.md"
    reconciler.generate_reconciliation_report(report_path)

    row_counts = reconciler.check_row_counts()
    key_eq = reconciler.check_key_set_equivalence()
    aggregates = reconciler.check_aggregates()
    checksums = reconciler.check_deterministic_checksums()

    rec_table = Table(title="Cross-Strategy Equivalence Audit")
    rec_table.add_column("Strategy", style="cyan")
    rec_table.add_column("Rows", justify="right")
    rec_table.add_column("Total Sum ($)", justify="right")
    rec_table.add_column("MD5 Deterministic Hash", style="bold green")

    for strat in ["Full Refresh", "Incremental Merge", "Partition Replace"]:
        rec_table.add_row(
            strat,
            str(row_counts[strat]),
            f"${aggregates[strat]['sum_total_amount']:,.2f}",
            checksums[strat]
        )
    console.print(rec_table)

    all_matched = (
        len(set(row_counts.values())) == 1 and
        key_eq["is_identical"] and
        len(set(checksums.values())) == 1
    )

    if all_matched:
        console.print("\n[bold green][SUCCESS] RECONCILIATION: All 3 strategies produced 100% mathematically equivalent final states![/bold green]")
    else:
        console.print("\n[bold red][FAILURE] RECONCILIATION: Divergence detected between strategies![/bold red]")

    # 8. Idempotency Check
    console.print("\n[bold yellow]Step 6: Validating Load Idempotency (Rerunning Same Batch)...[/bold yellow]")
    count_before = reconciler.check_row_counts()
    pipe_full.run(accumulated_source_orders, batch_id="batch_002_rerun")
    pipe_inc.run(combined_delta, batch_id="batch_002_rerun")
    pipe_part.run(combined_delta, batch_id="batch_002_rerun")
    count_after = reconciler.check_row_counts()

    idempotent = all(count_before[s] == count_after[s] for s in count_before)
    if idempotent:
        console.print("  [bold green][SUCCESS] IDEMPOTENCY CONFIRMED: Re-executing identical batches generated zero duplicate rows.[/bold green]")
    else:
        console.print("  [bold red][FAILURE] IDEMPOTENCY FAILURE: Target row counts changed upon rerun![/bold red]")

    # 9. Audit History
    console.print("\n[bold yellow]Step 7: Operational Audit Log Summary...[/bold yellow]")
    audit_logger = AuditLogger(con)
    print(audit_logger.print_audit_summary(limit=6))

    # 10. Performance Benchmark
    console.print("\n[bold yellow]Step 8: Executing Performance Benchmarks & Optimization Profiling...[/bold yellow]")
    benchmarker = BenchmarkRunner(seed=42)
    bench_report_path = REPORTS_DIR / "performance_benchmark_report.md"
    benchmarker.run_benchmark(initial_count=5000, incremental_count=1000, output_report_path=bench_report_path)

    con.close()
    console.print("\n[bold green]End-to-End Pipeline Execution Finished Successfully![/bold green]")
    console.print(f"Reports saved to [cyan]{REPORTS_DIR}[/cyan]")

if __name__ == "__main__":
    run_end_to_end_demo()
