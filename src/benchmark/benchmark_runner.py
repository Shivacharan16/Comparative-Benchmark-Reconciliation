"""
Performance Benchmarking and Trade-off Measurement Engine.
Profiles runtime, throughput, memory, and scaling behavior across strategies,
and quantifies a concrete optimization (Columnar Partition Pruning / Indexing).
"""
import time
import tracemalloc
import duckdb
from typing import Dict, Any, List
from tabulate import tabulate
from pathlib import Path

from src.generator.data_generator import DatasetGenerator
from src.pipelines.full_refresh import FullRefreshPipeline
from src.pipelines.incremental_merge import IncrementalMergePipeline
from src.pipelines.partition_replace import PartitionReplacementPipeline
from src.reconciliation.reconciler import ReconciliationEngine
from config.settings import REPORTS_DIR

class BenchmarkRunner:
    """
    Executes comparative performance profiling across:
    - Full Refresh
    - Incremental Merge
    - Partition Replacement
    Measures duration, throughput (rows/sec), and peak memory usage.
    """

    def __init__(self, seed: int = 42):
        self.generator = DatasetGenerator(seed=seed)

    def run_benchmark(
        self,
        initial_count: int = 10000,
        incremental_count: int = 2000,
        output_report_path: Path = None
    ) -> Dict[str, Any]:
        """
        Executes benchmark with initial baseline followed by incremental delta batch.
        """
        print(f"\n[BENCHMARK] Starting Benchmark with Initial Batch = {initial_count:,} records, Delta = {incremental_count:,} records...")

        # 1. Generate data
        customers = self.generator.generate_customers(count=500)
        initial_orders = self.generator.generate_initial_orders(count=initial_count, start_date="2026-10-25", num_days=3)
        delta_orders, metrics = self.generator.generate_incremental_orders(
            existing_orders=initial_orders,
            new_inserts_count=incremental_count,
            updates_ratio=0.15,
            soft_deletes_ratio=0.02
        )

        # Full accumulated orders for Full Refresh
        # (Simulates source database state after delta respecting watermark)
        accumulated_dict = {o["order_id"]: o for o in initial_orders}
        for d in delta_orders:
            curr = accumulated_dict.get(d["order_id"])
            if curr is None or d["updated_at"] >= curr["updated_at"]:
                accumulated_dict[d["order_id"]] = d
        accumulated_orders = list(accumulated_dict.values())

        # In-memory DuckDB connection for benchmark isolation
        con = duckdb.connect(":memory:")

        # Initialize pipelines
        pipe_full = FullRefreshPipeline(con, "orders_full_refresh")
        pipe_inc = IncrementalMergePipeline(con, "orders_incremental_merge")
        pipe_part = PartitionReplacementPipeline(con, "orders_partition_replace")

        # Load reference dimension
        pipe_full.load_customers(customers)

        benchmark_results = {
            "initial_phase": {},
            "incremental_phase": {},
            "optimization_test": {}
        }

        # ==========================================
        # Phase 1: Baseline Load (Initial Ingestion)
        # ==========================================
        print("\n--- Running Phase 1: Baseline Load ---")

        # Benchmark Full Refresh Initial
        tracemalloc.start()
        t0 = time.perf_counter()
        pipe_full.run(initial_orders, batch_id="init_full")
        t1 = time.perf_counter()
        _, peak_mem_full1 = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        dur_full1 = t1 - t0
        benchmark_results["initial_phase"]["Full Refresh"] = {
            "duration_s": dur_full1,
            "throughput": int(len(initial_orders) / max(dur_full1, 0.0001)),
            "peak_mem_mb": round(peak_mem_full1 / (1024 * 1024), 2)
        }

        # Benchmark Incremental Merge Initial
        tracemalloc.start()
        t0 = time.perf_counter()
        pipe_inc.run(initial_orders, batch_id="init_inc")
        t1 = time.perf_counter()
        _, peak_mem_inc1 = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        dur_inc1 = t1 - t0
        benchmark_results["initial_phase"]["Incremental Merge"] = {
            "duration_s": dur_inc1,
            "throughput": int(len(initial_orders) / max(dur_inc1, 0.0001)),
            "peak_mem_mb": round(peak_mem_inc1 / (1024 * 1024), 2)
        }

        # Benchmark Partition Replacement Initial
        tracemalloc.start()
        t0 = time.perf_counter()
        pipe_part.run(initial_orders, batch_id="init_part")
        t1 = time.perf_counter()
        _, peak_mem_part1 = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        dur_part1 = t1 - t0
        benchmark_results["initial_phase"]["Partition Replace"] = {
            "duration_s": dur_part1,
            "throughput": int(len(initial_orders) / max(dur_part1, 0.0001)),
            "peak_mem_mb": round(peak_mem_part1 / (1024 * 1024), 2)
        }

        # ==========================================
        # Phase 2: Incremental Delta Update Load
        # ==========================================
        print("\n--- Running Phase 2: Incremental Delta Load ---")

        # In Phase 2, Full Refresh must process the ENTIRE dataset (initial + delta)
        tracemalloc.start()
        t0 = time.perf_counter()
        pipe_full.run(accumulated_orders, batch_id="delta_full")
        t1 = time.perf_counter()
        _, peak_mem_full2 = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        dur_full2 = t1 - t0
        benchmark_results["incremental_phase"]["Full Refresh"] = {
            "processed_records": len(accumulated_orders),
            "duration_s": dur_full2,
            "throughput": int(len(accumulated_orders) / max(dur_full2, 0.0001)),
            "peak_mem_mb": round(peak_mem_full2 / (1024 * 1024), 2)
        }

        # Incremental Merge processes ONLY the incoming delta
        tracemalloc.start()
        t0 = time.perf_counter()
        pipe_inc.run(delta_orders, batch_id="delta_inc")
        t1 = time.perf_counter()
        _, peak_mem_inc2 = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        dur_inc2 = t1 - t0
        benchmark_results["incremental_phase"]["Incremental Merge"] = {
            "processed_records": len(delta_orders),
            "duration_s": dur_inc2,
            "throughput": int(len(delta_orders) / max(dur_inc2, 0.0001)),
            "peak_mem_mb": round(peak_mem_inc2 / (1024 * 1024), 2)
        }

        # Partition Replace processes ONLY touched partitions in delta
        tracemalloc.start()
        t0 = time.perf_counter()
        pipe_part.run(delta_orders, batch_id="delta_part")
        t1 = time.perf_counter()
        _, peak_mem_part2 = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        dur_part2 = t1 - t0
        benchmark_results["incremental_phase"]["Partition Replace"] = {
            "processed_records": len(delta_orders),
            "duration_s": dur_part2,
            "throughput": int(len(delta_orders) / max(dur_part2, 0.0001)),
            "peak_mem_mb": round(peak_mem_part2 / (1024 * 1024), 2)
        }

        # ==========================================
        # Phase 3: Equivalence Verification
        # ==========================================
        print("\n--- Running Phase 3: Equivalence Check ---")
        reconciler = ReconciliationEngine(con)
        report_md = reconciler.generate_reconciliation_report(REPORTS_DIR / "benchmark_reconciliation.md")

        # ==========================================
        # Phase 4: Reasonable Optimization Demonstration
        # (Columnar Projection & Partition Pruning Pushdown)
        # ==========================================
        print("\n--- Running Phase 4: Optimization Demonstration (Columnar Parquet Projection Pushdown) ---")
        # Save dataset to Parquet to demonstrate columnar vectorization & predicate pushdown
        parquet_sample_path = REPORTS_DIR / "orders_benchmark_sample.parquet"
        con.execute(f"COPY (SELECT * FROM orders_incremental_merge) TO '{str(parquet_sample_path).replace('\\', '/')}' (FORMAT PARQUET);")

        # Baseline: Full table scan with all columns loaded
        t0 = time.perf_counter()
        for _ in range(30):
            con.execute(f"SELECT * FROM read_parquet('{str(parquet_sample_path).replace('\\', '/')}') WHERE order_date = '2026-10-26';").fetchall()
        t1 = time.perf_counter()
        dur_unoptimized = (t1 - t0) / 30.0

        # Optimized: Columnar projection pushdown (only requested analytical aggregate columns read from disk)
        t0 = time.perf_counter()
        for _ in range(30):
            con.execute(f"SELECT sum(total_amount), count(*) FROM read_parquet('{str(parquet_sample_path).replace('\\', '/')}') WHERE order_date = '2026-10-26';").fetchall()
        t1 = time.perf_counter()
        dur_optimized = (t1 - t0) / 30.0

        speedup = round(dur_unoptimized / max(dur_optimized, 0.000001), 2)
        benchmark_results["optimization_test"] = {
            "unoptimized_avg_ms": round(dur_unoptimized * 1000, 3),
            "optimized_avg_ms": round(dur_optimized * 1000, 3),
            "speedup_factor": f"{speedup}x"
        }

        # Formatted Output
        formatted_summary = self._format_summary(benchmark_results, initial_count, incremental_count)
        print(formatted_summary)

        if output_report_path:
            output_report_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_report_path, "w", encoding="utf-8") as f:
                f.write(formatted_summary + "\n\n" + report_md)

        con.close()
        return benchmark_results

    def _format_summary(self, res: Dict[str, Any], init_cnt: int, inc_cnt: int) -> str:
        lines = []
        lines.append("# Performance Benchmark & Runtime Trade-off Report")
        lines.append(f"**Workload Configuration:** Initial = {init_cnt:,} records | Delta = {inc_cnt:,} records\n")

        lines.append("## 1. Initial Ingestion Runtime (Cold Load)")
        init_table = []
        for strat, m in res["initial_phase"].items():
            init_table.append([strat, f"{m['duration_s']:.4f} s", f"{m['throughput']:,} rows/s", f"{m['peak_mem_mb']} MB"])
        lines.append(tabulate(init_table, headers=["Strategy", "Duration", "Throughput", "Peak Memory"], tablefmt="github"))
        lines.append("")

        lines.append("## 2. Incremental Delta Update Runtime (CDC Load)")
        inc_table = []
        for strat, m in res["incremental_phase"].items():
            inc_table.append([strat, f"{m['processed_records']:,}", f"{m['duration_s']:.4f} s", f"{m['throughput']:,} rows/s", f"{m['peak_mem_mb']} MB"])
        lines.append(tabulate(inc_table, headers=["Strategy", "Processed Records", "Duration", "Throughput", "Peak Memory"], tablefmt="github"))
        lines.append("")

        lines.append("## 3. Optimization Measurement: Columnar Projection Pushdown in Parquet")
        opt = res["optimization_test"]
        opt_table = [
            ["Full Row Materialization (SELECT * Query)", f"{opt['unoptimized_avg_ms']} ms", "1.0x (Baseline)"],
            ["Columnar Projection Pushdown (Aggregates Only)", f"{opt['optimized_avg_ms']} ms", f"{opt['speedup_factor']} Speedup"]
        ]
        lines.append(tabulate(opt_table, headers=["Technique", "Average Latency", "Speedup"], tablefmt="github"))
        lines.append("")

        return "\n".join(lines)
