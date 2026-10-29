# Cross-Strategy Pipeline Reconciliation Report

**Overall Status:** PASSED (100% EQUIVALENT)

## 1. Target Row Counts
| Strategy          |   Target Total Rows |
|-------------------|---------------------|
| Full Refresh      |                1202 |
| Incremental Merge |                1202 |
| Partition Replace |                1202 |

## 2. Primary Key Set Equivalence
- **Key Sets Match Identically:** YES

## 3. Financial & Volume Aggregates
| Strategy          |   Rows | Sum Amount   | Sum Tax     | Sum Total     |   Total Items |   Customers |
|-------------------|--------|--------------|-------------|---------------|---------------|-------------|
| Full Refresh      |   1202 | $957,878.04  | $172,417.99 | $1,130,296.03 |          7643 |         293 |
| Incremental Merge |   1202 | $957,878.04  | $172,417.99 | $1,130,296.03 |          7643 |         293 |
| Partition Replace |   1202 | $957,878.04  | $172,417.99 | $1,130,296.03 |          7643 |         293 |

## 4. Partition-Level Granular Reconciliation (By Date)
| Date       |   Full Rows |   Inc Rows |   Part Rows | Full Sum    | Inc Sum     | Part Sum    | Status   |
|------------|-------------|------------|-------------|-------------|-------------|-------------|----------|
| 2026-10-25 |         320 |        320 |         320 | $287,904.17 | $287,904.17 | $287,904.17 | MATCH    |
| 2026-10-26 |         323 |        323 |         323 | $279,112.58 | $279,112.58 | $279,112.58 | MATCH    |
| 2026-10-27 |         357 |        357 |         357 | $323,510.08 | $323,510.08 | $323,510.08 | MATCH    |
| 2026-10-28 |         202 |        202 |         202 | $239,769.20 | $239,769.20 | $239,769.20 | MATCH    |

## 5. Deterministic MD5 Checksums (Sorted State)
| Strategy          | MD5 Checksum Hash                |
|-------------------|----------------------------------|
| Full Refresh      | 88a445a307dcab1cb9c67ad3a2f5ebb8 |
| Incremental Merge | 88a445a307dcab1cb9c67ad3a2f5ebb8 |
| Partition Replace | 88a445a307dcab1cb9c67ad3a2f5ebb8 |
