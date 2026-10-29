# Architecture & Design Flow

This document details the architectural design, ingestion lifecycle, loading strategy mechanics, transaction isolation, and observability model implemented in the Data Engineering Pipeline.

---

## 1. High-Level Architecture Overview

The pipeline ingests raw event batches (JSONL or Parquet), enforces schema validation, verifies domain business rules, handles intra-batch duplicates, quarantines bad records, and publishes valid data to an analytical warehouse using three distinct loading strategies.

```mermaid
flowchart TD
    subgraph S1["1. Raw Ingestion"]
        RAW["Raw Event Stream / Files<br>(JSONL, Parquet)"]
        DIM["Dimension Lookups<br>(dim_customers)"]
    end

    subgraph S2["2. Validation & Quarantine Engine"]
        VAL{"BatchValidator<br>Schema & Business Rules"}
        DEDUP["Intra-Batch Deduplication<br>(Latest updated_at Wins)"]
        QUAR["Quarantine Sink<br>(quarantine_*.parquet & .jsonl)"]
        CLEAN["Validated Orders<br>+ Derived Tax & Total"]
    end

    subgraph S3["3. Ingestion Strategies"]
        direction TB
        F1["Strategy A: Full Refresh<br>(Truncate & Atomic Swap)"]
        F2["Strategy B: Incremental Merge<br>(SQL MERGE / UPSERT + Watermark)"]
        F3["Strategy C: Partition Replace<br>(Dynamic Partition Consolidation)"]
    end

    subgraph S4["4. Analytical Storage Layer"]
        T1[("orders_full_refresh")]
        T2[("orders_incremental_merge")]
        T3[("orders_partition_replace")]
    end

    subgraph S5["5. Observability & Verification"]
        AUDIT[("pipeline_audit_log<br>(Lineage, Metrics, Status)")]
        REC["Reconciliation Engine<br>(Row counts, Aggregates, MD5 Checksums)"]
    end

    RAW --> VAL
    DIM --> VAL
    VAL -- "Malformed / Bad FK / Rule Fail" --> QUAR
    VAL -- "Valid Rows" --> DEDUP
    DEDUP -- "Shadowed Older Duplicates" --> QUAR
    DEDUP -- "Processed Valid Records" --> CLEAN

    CLEAN --> F1 --> T1
    CLEAN --> F2 --> T2
    CLEAN --> F3 --> T3

    F1 -.-> AUDIT
    F2 -.-> AUDIT
    F3 -.-> AUDIT

    T1 & T2 & T3 --> REC
```

---

## 2. Ingestion Lifecycle & Step-by-Step Flow

Every pipeline execution follows a structured five-stage lifecycle defined in `BasePipeline`:

1. **Initialization & Lineage Tracking**:
   - Generates a unique `run_id` (e.g. `run_749336814437`).
   - Caches active dimension reference keys from `dim_customers` for fast in-memory referential integrity checks.
   - Begins execution timer.

2. **Validation & Normalization (`BatchValidator`)**:
   - **Type & Schema Validation**: Pydantic models validate data types, non-null constraints, and ISO-8601 timestamps.
   - **Business Rules**: Checks that `amount > 0.0` and `items_count >= 1`.
   - **Referential Integrity**: Checks that `customer_id` exists in `dim_customers`.
   - **Derived Fields**: Computes `tax_amount = ROUND(amount * 0.18, 2)` and `total_amount = ROUND(amount + tax_amount, 2)`.
   - **Intra-Batch Deduplication**: When multiple events for the same `order_id` arrive in one batch, they are sorted by `updated_at DESC`. The freshest event is accepted; shadowed older events are routed to quarantine.

3. **Quarantine Routing**:
   - All invalid or shadowed records are written immediately to `data/quarantine/quarantine_<run_id>.parquet` and `.jsonl`.
   - Preserves original raw payload, error category, failure reason, and batch origin for triage.

4. **Strategy-Specific Target Publishing**:
   - Executes the chosen loading pattern within an atomic ACID transaction.

5. **Operational Audit Recording (`AuditLogger`)**:
   - Records execution status (`SUCCESS` / `FAILED`), start/end time, duration, input counts, valid counts, quarantine counts, target counts, and any exception stack traces to `pipeline_audit_log`.

---

## 3. Deep Dive: The Three Loading Strategies

### Strategy A: Full Refresh (Atomic Table Swap)
- **Concept**: Truncates and rebuilds the target table from scratch on each run.
- **Atomic Swap Mechanism**:
  1. Creates an isolated staging table: `orders_full_refresh_staging_<run_id>`.
  2. Bulk inserts all active records into staging via PyArrow zero-copy buffer.
  3. Executes an atomic transaction:
     ```sql
     BEGIN TRANSACTION;
     DROP TABLE IF EXISTS orders_full_refresh_old;
     ALTER TABLE IF EXISTS orders_full_refresh RENAME TO orders_full_refresh_old;
     ALTER TABLE orders_full_refresh_staging RENAME TO orders_full_refresh;
     DROP TABLE IF EXISTS orders_full_refresh_old;
     COMMIT;
     ```
- **Advantages**:
  - Zero downtime: Analytical queries reading `orders_full_refresh` never see an empty table during the load.
  - Complete self-healing: Automatically heals corrupted, deleted, or drifting rows.
  - Zero state tracking required.
- **Drawbacks**:
  - $O(N)$ execution runtime where $N$ is total historical volume. Compute, network, and storage I/O scale linearly with historical dataset size.

---

### Strategy B: Incremental Merge (UPSERT / CDC with Watermark)
- **Concept**: Reads only changed/new records since the previous watermark and merges them using the primary key `order_id`.
- **Watermark & Out-of-Order Protection**:
  When event streams replay or network packets arrive out of order, a stale event with an older timestamp must never overwrite fresher data already committed in the target table.
- **SQL MERGE Implementation**:
  ```sql
  MERGE INTO orders_incremental_merge AS target
  USING staging_merge AS src
  ON target.order_id = src.order_id
  WHEN MATCHED AND src.updated_at >= target.updated_at THEN
      UPDATE SET
          customer_id = src.customer_id,
          order_date = src.order_date,
          order_timestamp = src.order_timestamp,
          order_status = src.order_status,
          amount = src.amount,
          tax_amount = src.tax_amount,
          total_amount = src.total_amount,
          updated_at = src.updated_at,
          is_deleted = src.is_deleted,
          run_id = src.run_id,
          source_file = src.source_file,
          ingested_at = src.ingested_at
  WHEN NOT MATCHED THEN
      INSERT (order_id, customer_id, order_date, ...)
      VALUES (src.order_id, src.customer_id, src.order_date, ...);
  ```
- **Advantages**:
  - $O(\Delta N)$ runtime: Processes only incoming delta rows ($\Delta N \ll N$).
  - Up to 3x faster runtime and 3x less memory consumption on incremental batches.
  - Granular updates across any historical date without partition rebuilding.
- **Drawbacks**:
  - Requires primary key indexing and write-ahead transaction log.
  - Requires careful watermark handling to prevent out-of-order data corruption.

---

### Strategy C: Partition Replacement (Dynamic Partition Consolidation)
- **Concept**: Partitions analytical target data by `order_date`. Replaces data on a partition-by-partition basis, leaving all untouched historical partitions unmodified.
- **Partition Consolidation Pattern**:
  In real-world CDC streams, an incoming batch may contain updates for orders in partition `2026-10-26`, but NOT all orders of `2026-10-26`. A naive partition overwrite (`DELETE WHERE order_date = '2026-10-26'; INSERT INTO target SELECT * FROM batch`) would delete untouched historical records in that partition!
  Our pipeline implements **Consolidated Partition Replacement**:
  1. Identifies touched partition dates: $P = \{ p_1, p_2, \dots \}$ from the batch.
  2. Queries existing rows from target for dates in $P$.
  3. Merges existing rows with incoming delta in staging (respecting the watermark `src.updated_at >= target.updated_at`).
  4. Executes atomic replacement inside a transaction:
     ```sql
     BEGIN TRANSACTION;
     DELETE FROM orders_partition_replace WHERE CAST(order_date AS VARCHAR) IN ('2026-10-25', '2026-10-26');
     INSERT INTO orders_partition_replace SELECT * FROM staging_part_consolidated;
     COMMIT;
     ```
- **Advantages**:
  - Isolates blast radius strictly to touched partitions.
  - Untouched historical partitions are neither scanned nor rewritten.
  - Fits object-store data lake architectures (S3, GCS, ADLS) and Hive partition directories.
- **Drawbacks**:
  - If batches touch all historical partitions, performance degrades to near full refresh.
  - Requires partition consolidation when delta batches are not partition-complete.

---

## 4. Failure Recovery & Idempotency Design

### Idempotency Guarantee
Every strategy is built to be strictly idempotent. Re-running the identical batch $k$ times produces identical target table states:
- **Full Refresh**: Completely replaces the table with the incoming snapshot. Identical inputs produce identical tables.
- **Incremental Merge**: Merging identical records satisfies `src.updated_at >= target.updated_at` with identical values, leaving the row contents unchanged and creating zero duplicate rows.
- **Partition Replacement**: Overwriting the partition with consolidated records replaces the partition with identical rows.

### Transaction Rollback & Failure Isolation
All target table mutations are wrapped in database transaction blocks (`BEGIN TRANSACTION` / `COMMIT`). If an unexpected failure occurs (e.g. disk failure, system interruption, or validation constraint breach):
- The transaction issues an immediate `ROLLBACK`.
- Target tables remain intact in their prior committed state.
- The failure is recorded in `pipeline_audit_log` with `status = 'FAILED'` and the exact error traceback.
