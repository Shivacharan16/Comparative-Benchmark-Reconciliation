"""
Command Line Interface (CLI) for Data Ingestion Strategy Benchmark.
"""
import argparse
import sys
import duckdb
from pathlib import Path
from rich.console import Console

from config.settings import (
    DATABASE_PATH,
    RAW_DATA_DIR,
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
from run_demo import run_end_to_end_demo

console = Console()

def cmd_generate(args):
    console.print(f"[bold cyan]Generating synthetic datasets (count={args.count:,})...[/bold cyan]")
    generator = DatasetGenerator(seed=args.seed)
    generator.generate_customers(count=max(200, args.count // 10), save_path=RAW_DATA_DIR / "customers.jsonl")
    generator.generate_initial_orders(count=args.count, save_path=RAW_DATA_DIR / "initial_orders.jsonl")
    generator.generate_edge_case_batch(save_path=RAW_DATA_DIR / "edge_case_orders.jsonl")
    if args.parquet_100k:
        console.print("[cyan]Generating 100,000+ record Parquet file...[/cyan]")
        pq_path = generator.generate_large_scale_dataset(count=100000, output_parquet_path=RAW_DATA_DIR / "orders_100k.parquet")
        console.print(f"Parquet file written to: {pq_path}")
    console.print(f"[green][OK] Data generation completed in {RAW_DATA_DIR}[/green]")

def cmd_benchmark(args):
    benchmarker = BenchmarkRunner(seed=args.seed)
    bench_report_path = REPORTS_DIR / "performance_benchmark_report.md"
    benchmarker.run_benchmark(
        initial_count=args.initial,
        incremental_count=args.delta,
        output_report_path=bench_report_path
    )
    console.print(f"[green][OK] Benchmark completed. Report written to {bench_report_path}[/green]")

def cmd_reconcile(args):
    con = duckdb.connect(str(DATABASE_PATH))
    reconciler = ReconciliationEngine(con)
    report = reconciler.generate_reconciliation_report(REPORTS_DIR / "cli_reconciliation_report.md")
    console.print(report)
    con.close()

def cmd_audit(args):
    con = duckdb.connect(str(DATABASE_PATH))
    logger = AuditLogger(con)
    console.print(logger.print_audit_summary(limit=args.limit))
    con.close()

def main():
    parser = argparse.ArgumentParser(description="Ingestion Strategy Comparison CLI")
    subparsers = parser.add_subparsers(dest="command")

    # Demo
    subparsers.add_parser("demo", help="Run full end-to-end demo")

    # Generate
    p_gen = subparsers.add_parser("generate", help="Generate synthetic datasets")
    p_gen.add_argument("--count", type=int, default=1000, help="Initial order count")
    p_gen.add_argument("--seed", type=int, default=42, help="Random seed")
    p_gen.add_argument("--parquet-100k", action="store_true", help="Generate 100k+ Parquet dataset")

    # Benchmark
    p_bench = subparsers.add_parser("benchmark", help="Run performance benchmarks")
    p_bench.add_argument("--initial", type=int, default=10000, help="Initial baseline records")
    p_bench.add_argument("--delta", type=int, default=2000, help="Incremental delta records")
    p_bench.add_argument("--seed", type=int, default=42, help="Random seed")

    # Reconcile
    subparsers.add_parser("reconcile", help="Run cross-strategy reconciliation")

    # Audit
    p_audit = subparsers.add_parser("audit", help="Print operational audit logs")
    p_audit.add_argument("--limit", type=int, default=10, help="Number of records to show")

    args = parser.parse_args()

    if args.command == "demo" or args.command is None:
        run_end_to_end_demo()
    elif args.command == "generate":
        cmd_generate(args)
    elif args.command == "benchmark":
        cmd_benchmark(args)
    elif args.command == "reconcile":
        cmd_reconcile(args)
    elif args.command == "audit":
        cmd_audit(args)

if __name__ == "__main__":
    main()
