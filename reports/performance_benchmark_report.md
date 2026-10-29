# Performance Benchmark & Runtime Trade-off Report
**Workload Configuration:** Initial = 5,000 records | Delta = 1,000 records

## 1. Initial Ingestion Runtime (Cold Load)
| Strategy          | Duration   | Throughput   | Peak Memory   |
|-------------------|------------|--------------|---------------|
| Full Refresh      | 0.8268 s   | 6,047 rows/s | 13.4 MB       |
| Incremental Merge | 1.1123 s   | 4,495 rows/s | 13.39 MB      |
| Partition Replace | 0.8179 s   | 6,112 rows/s | 13.41 MB      |

## 2. Incremental Delta Update Runtime (CDC Load)
| Strategy          |   Processed Records | Duration   | Throughput   | Peak Memory   |
|-------------------|---------------------|------------|--------------|---------------|
| Full Refresh      |               6,025 | 1.1713 s   | 5,143 rows/s | 16.17 MB      |
| Incremental Merge |               1,890 | 0.4849 s   | 3,897 rows/s | 5.05 MB       |
| Partition Replace |               1,890 | 0.4112 s   | 4,596 rows/s | 5.05 MB       |

## 3. Optimization Measurement: Columnar Projection Pushdown in Parquet
| Technique                                      | Average Latency   | Speedup         |
|------------------------------------------------|-------------------|-----------------|
| Full Row Materialization (SELECT * Query)      | 8.406 ms          | 1.0x (Baseline) |
| Columnar Projection Pushdown (Aggregates Only) | 2.335 ms          | 3.6x Speedup    |


# Cross-Strategy Pipeline Reconciliation Report

**Overall Status:** PASSED (100% EQUIVALENT)

## 1. Target Row Counts
| Strategy          |   Target Total Rows |
|-------------------|---------------------|
| Full Refresh      |                6000 |
| Incremental Merge |                6000 |
| Partition Replace |                6000 |

## 2. Primary Key Set Equivalence
- **Key Sets Match Identically:** YES

## 3. Financial & Volume Aggregates
| Strategy          |   Rows | Sum Amount    | Sum Tax     | Sum Total     |   Total Items |   Customers |
|-------------------|--------|---------------|-------------|---------------|---------------|-------------|
| Full Refresh      |   6000 | $4,793,148.49 | $862,766.65 | $5,655,915.14 |         38356 |         500 |
| Incremental Merge |   6000 | $4,793,148.49 | $862,766.65 | $5,655,915.14 |         38356 |         500 |
| Partition Replace |   6000 | $4,793,148.49 | $862,766.65 | $5,655,915.14 |         38356 |         500 |

## 4. Partition-Level Granular Reconciliation (By Date)
| Date       |   Full Rows |   Inc Rows |   Part Rows | Full Sum      | Inc Sum       | Part Sum      | Status   |
|------------|-------------|------------|-------------|---------------|---------------|---------------|----------|
| 2026-10-25 |        1691 |       1691 |        1691 | $1,493,694.62 | $1,493,694.62 | $1,493,694.62 | MATCH    |
| 2026-10-26 |        1680 |       1680 |        1680 | $1,479,799.76 | $1,479,799.76 | $1,479,799.76 | MATCH    |
| 2026-10-27 |        1629 |       1629 |        1629 | $1,472,405.41 | $1,472,405.41 | $1,472,405.41 | MATCH    |
| 2026-10-28 |        1000 |       1000 |        1000 | $1,210,015.35 | $1,210,015.35 | $1,210,015.35 | MATCH    |

## 5. Deterministic MD5 Checksums (Sorted State)
| Strategy          | MD5 Checksum Hash                |
|-------------------|----------------------------------|
| Full Refresh      | 40b459fd09520daf3faf7d74e67e1833 |
| Incremental Merge | 40b459fd09520daf3faf7d74e67e1833 |
| Partition Replace | 40b459fd09520daf3faf7d74e67e1833 |
