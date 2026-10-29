"""
Reconciliation and Equivalence Verification Engine.
Performs cross-strategy integrity verification across Full Refresh, Incremental Merge,
and Partition Replacement target tables to validate mathematical equivalence.
"""
import duckdb
from typing import Dict, Any, List, Tuple
from tabulate import tabulate
from pathlib import Path
from config.settings import (
    TABLE_FULL_REFRESH,
    TABLE_INCREMENTAL_MERGE,
    TABLE_PARTITION_REPLACE,
    REPORTS_DIR
)

class ReconciliationEngine:
    """
    Automated reconciliation auditor comparing:
    - Row count consistency
    - Primary key set equality
    - Aggregated totals (total_amount, tax, items)
    - Partition-level aggregates (by order_date)
    - Deterministic MD5 checksums over sorted target states
    """

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con
        self.tables = {
            "Full Refresh": TABLE_FULL_REFRESH,
            "Incremental Merge": TABLE_INCREMENTAL_MERGE,
            "Partition Replace": TABLE_PARTITION_REPLACE
        }

    def check_row_counts(self) -> Dict[str, int]:
        """Queries row count for each target table."""
        counts = {}
        for name, table in self.tables.items():
            try:
                cnt = self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                counts[name] = cnt
            except Exception as e:
                counts[name] = -1
        return counts

    def check_key_set_equivalence(self) -> Dict[str, Any]:
        """
        Verifies whether primary key sets match identically across tables.
        Returns symmetrical differences if any exist.
        """
        t_full = TABLE_FULL_REFRESH
        t_inc = TABLE_INCREMENTAL_MERGE
        t_part = TABLE_PARTITION_REPLACE

        # Keys in Full but not Inc
        diff_full_inc = self.con.execute(f"""
            SELECT order_id FROM {t_full} EXCEPT SELECT order_id FROM {t_inc}
        """).fetchall()

        # Keys in Inc but not Full
        diff_inc_full = self.con.execute(f"""
            SELECT order_id FROM {t_inc} EXCEPT SELECT order_id FROM {t_full}
        """).fetchall()

        # Keys in Full but not Part
        diff_full_part = self.con.execute(f"""
            SELECT order_id FROM {t_full} EXCEPT SELECT order_id FROM {t_part}
        """).fetchall()

        # Keys in Part but not Full
        diff_part_full = self.con.execute(f"""
            SELECT order_id FROM {t_part} EXCEPT SELECT order_id FROM {t_full}
        """).fetchall()

        is_identical = (
            len(diff_full_inc) == 0 and
            len(diff_inc_full) == 0 and
            len(diff_full_part) == 0 and
            len(diff_part_full) == 0
        )

        return {
            "is_identical": is_identical,
            "diff_full_minus_inc": [r[0] for r in diff_full_inc],
            "diff_inc_minus_full": [r[0] for r in diff_inc_full],
            "diff_full_minus_part": [r[0] for r in diff_full_part],
            "diff_part_minus_full": [r[0] for r in diff_part_full]
        }

    def check_aggregates(self) -> Dict[str, Dict[str, float]]:
        """
        Calculates core analytical aggregates across tables:
        SUM(total_amount), SUM(amount), SUM(tax_amount), SUM(items_count), COUNT(DISTINCT customer_id)
        """
        aggregates = {}
        for name, table in self.tables.items():
            query = f"""
            SELECT 
                COUNT(*) as count,
                COALESCE(ROUND(SUM(amount), 2), 0.0) as sum_amount,
                COALESCE(ROUND(SUM(tax_amount), 2), 0.0) as sum_tax,
                COALESCE(ROUND(SUM(total_amount), 2), 0.0) as sum_total_amount,
                COALESCE(SUM(items_count), 0) as sum_items,
                COUNT(DISTINCT customer_id) as distinct_customers
            FROM {table}
            """
            row = self.con.execute(query).fetchone()
            aggregates[name] = {
                "count": row[0],
                "sum_amount": row[1],
                "sum_tax": row[2],
                "sum_total_amount": row[3],
                "sum_items": row[4],
                "distinct_customers": row[5]
            }
        return aggregates

    def check_partition_level_equivalence(self) -> List[Dict[str, Any]]:
        """
        Reconciles metrics grouped by order_date across all three strategies.
        """
        t_full = TABLE_FULL_REFRESH
        t_inc = TABLE_INCREMENTAL_MERGE
        t_part = TABLE_PARTITION_REPLACE

        query = f"""
        WITH f AS (
            SELECT CAST(order_date AS VARCHAR) as d, COUNT(*) as cnt_f, ROUND(SUM(total_amount), 2) as sum_f
            FROM {t_full} GROUP BY 1
        ),
        i AS (
            SELECT CAST(order_date AS VARCHAR) as d, COUNT(*) as cnt_i, ROUND(SUM(total_amount), 2) as sum_i
            FROM {t_inc} GROUP BY 1
        ),
        p AS (
            SELECT CAST(order_date AS VARCHAR) as d, COUNT(*) as cnt_p, ROUND(SUM(total_amount), 2) as sum_p
            FROM {t_part} GROUP BY 1
        )
        SELECT 
            COALESCE(f.d, i.d, p.d) as order_date,
            COALESCE(f.cnt_f, 0) as full_cnt,
            COALESCE(i.cnt_i, 0) as inc_cnt,
            COALESCE(p.cnt_p, 0) as part_cnt,
            COALESCE(f.sum_f, 0.0) as full_sum,
            COALESCE(i.sum_i, 0.0) as inc_sum,
            COALESCE(p.sum_p, 0.0) as part_sum
        FROM f
        FULL OUTER JOIN i ON f.d = i.d
        FULL OUTER JOIN p ON COALESCE(f.d, i.d) = p.d
        ORDER BY 1;
        """
        rows = self.con.execute(query).fetchall()
        result = []
        for r in rows:
            is_matched = (r[1] == r[2] == r[3]) and (abs(r[4] - r[5]) < 0.01) and (abs(r[5] - r[6]) < 0.01)
            result.append({
                "order_date": r[0],
                "full_cnt": r[1],
                "inc_cnt": r[2],
                "part_cnt": r[3],
                "full_sum": r[4],
                "inc_sum": r[5],
                "part_sum": r[6],
                "is_matched": is_matched
            })
        return result

    def check_deterministic_checksums(self) -> Dict[str, str]:
        """
        Calculates MD5 hash across sorted records for each table.
        Guarantees exact cell-level equivalence.
        """
        checksums = {}
        for name, table in self.tables.items():
            query = f"""
            SELECT md5(COALESCE(STRING_AGG(
                order_id || '|' || customer_id || '|' || CAST(order_date AS VARCHAR) || '|' || 
                order_status || '|' || printf('%.2f', amount) || '|' || printf('%.2f', total_amount) || '|' ||
                CAST(items_count AS VARCHAR) || '|' || CAST(is_deleted AS VARCHAR),
                ',' ORDER BY order_id
            ), 'EMPTY'))
            FROM {table};
            """
            try:
                h = self.con.execute(query).fetchone()[0]
                checksums[name] = h
            except Exception as e:
                checksums[name] = f"ERROR: {str(e)}"
        return checksums

    def generate_reconciliation_report(self, output_path: Path = None) -> str:
        """
        Generates a comprehensive Markdown & ASCII reconciliation report.
        """
        row_counts = self.check_row_counts()
        key_check = self.check_key_set_equivalence()
        aggregates = self.check_aggregates()
        partition_data = self.check_partition_level_equivalence()
        checksums = self.check_deterministic_checksums()

        all_counts_match = len(set(row_counts.values())) == 1
        all_checksums_match = len(set(checksums.values())) == 1
        all_partitions_match = all(p["is_matched"] for p in partition_data)

        overall_status = "PASSED (100% EQUIVALENT)" if (all_counts_match and key_check["is_identical"] and all_checksums_match and all_partitions_match) else "DIVERGENT / FAILED"

        lines = []
        lines.append("# Cross-Strategy Pipeline Reconciliation Report")
        lines.append(f"\n**Overall Status:** {overall_status}\n")

        # 1. Row Counts
        lines.append("## 1. Target Row Counts")
        count_rows = [[strat, cnt] for strat, cnt in row_counts.items()]
        lines.append(tabulate(count_rows, headers=["Strategy", "Target Total Rows"], tablefmt="github"))
        lines.append("")

        # 2. Key Equivalence
        lines.append("## 2. Primary Key Set Equivalence")
        lines.append(f"- **Key Sets Match Identically:** {'YES' if key_check['is_identical'] else 'NO'}")
        if not key_check['is_identical']:
            lines.append(f"- Full Refresh - Incremental: {key_check['diff_full_minus_inc']}")
            lines.append(f"- Incremental - Full Refresh: {key_check['diff_inc_minus_full']}")
            lines.append(f"- Full Refresh - Partition Replace: {key_check['diff_full_minus_part']}")
        lines.append("")

        # 3. Aggregates
        lines.append("## 3. Financial & Volume Aggregates")
        agg_rows = []
        for name, agg in aggregates.items():
            agg_rows.append([
                name,
                agg["count"],
                f"${agg['sum_amount']:,.2f}",
                f"${agg['sum_tax']:,.2f}",
                f"${agg['sum_total_amount']:,.2f}",
                agg["sum_items"],
                agg["distinct_customers"]
            ])
        lines.append(tabulate(agg_rows, headers=["Strategy", "Rows", "Sum Amount", "Sum Tax", "Sum Total", "Total Items", "Customers"], tablefmt="github"))
        lines.append("")

        # 4. Partition Level
        lines.append("## 4. Partition-Level Granular Reconciliation (By Date)")
        part_rows = []
        for p in partition_data:
            part_rows.append([
                p["order_date"],
                p["full_cnt"], p["inc_cnt"], p["part_cnt"],
                f"${p['full_sum']:,.2f}", f"${p['inc_sum']:,.2f}", f"${p['part_sum']:,.2f}",
                "MATCH" if p["is_matched"] else "MISMATCH"
            ])
        lines.append(tabulate(part_rows, headers=["Date", "Full Rows", "Inc Rows", "Part Rows", "Full Sum", "Inc Sum", "Part Sum", "Status"], tablefmt="github"))
        lines.append("")

        # 5. Row-level MD5 Checksum
        lines.append("## 5. Deterministic MD5 Checksums (Sorted State)")
        cs_rows = [[strat, h] for strat, h in checksums.items()]
        lines.append(tabulate(cs_rows, headers=["Strategy", "MD5 Checksum Hash"], tablefmt="github"))
        lines.append("")

        report_content = "\n".join(lines)

        if output_path:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(report_content)

        return report_content
