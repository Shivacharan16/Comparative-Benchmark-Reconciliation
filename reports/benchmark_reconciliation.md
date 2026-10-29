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
