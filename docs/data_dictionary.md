# Data Dictionary & Schema Specification

This document defines the entities, keys, grain, attributes, constraints, derivation rules, and audit metadata used across the Data Engineering ingestion pipelines.

---

## 1. Entity Overview & Grain

| Entity | Role | Physical Table / File | Grain | Primary Key | Partition Key |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Customer** | Dimension | `dim_customers` | One record per unique registered customer | `customer_id` | N/A |
| **Order Fact** | Fact / Target | `orders_full_refresh`<br>`orders_incremental_merge`<br>`orders_partition_replace` | One record per e-commerce order (latest state) | `order_id` | `order_date` (`YYYY-MM-DD`) |
| **Quarantine** | Error Sink | `data/quarantine/quarantine_*.parquet` | One record per failed or rejected ingestion event | Synthetic row index + `run_id` | `rejected_at` |
| **Audit Log** | Lineage / Ops | `pipeline_audit_log` | One record per pipeline execution run | `run_id` | N/A |

---

## 2. Table Specifications

### 2.1 Dimension: `dim_customers`
Reference dataset used for validating referential integrity on inbound transactions.

| Column Name | SQL Type | Nullable | Primary Key | Foreign Key | Description / Business Rules |
| :--- | :--- | :---: | :---: | :---: | :--- |
| `customer_id` | `VARCHAR` | **No** | **PK** | None | Stable deterministic identifier (`CUST-00001` to `CUST-99999`). |
| `customer_name` | `VARCHAR` | **No** | No | None | Customer legal or display name. |
| `customer_email`| `VARCHAR` | **No** | No | None | Contact email address format. |
| `customer_tier` | `VARCHAR` | **No** | No | None | Customer loyalty tier: `BRONZE`, `SILVER`, `GOLD`, `PLATINUM`. |
| `country_code`  | `VARCHAR` | **No** | No | None | ISO 3166-1 alpha-2 or alpha-3 country code (e.g., `US`, `IN`, `DE`, `GB`). |
| `created_at`    | `TIMESTAMP` | **No** | No | None | Customer account creation timestamp (ISO-8601 format). |

---

### 2.2 Analytical Fact: `orders_*` (All Target Strategies)
Target analytical orders table populated by `FullRefreshPipeline`, `IncrementalMergePipeline`, and `PartitionReplacementPipeline`.

| Column Name | SQL Type | Nullable | Constraints / Domain | Description & Derivation Logic |
| :--- | :--- | :---: | :--- | :--- |
| `order_id` | `VARCHAR` | **No** | Primary Key, Non-empty | Unique deterministic order business key (`ORD-YYYYMMDD-XXXXXX`). |
| `customer_id` | `VARCHAR` | **No** | FK -> `dim_customers(customer_id)` | Reference to purchasing customer. Must exist in `dim_customers`. |
| `order_date` | `DATE` | **No** | Format `YYYY-MM-DD` | Logical calendar date of the order; primary partition key. |
| `order_timestamp` | `TIMESTAMP` | **No** | ISO-8601 UTC | Exact time the order was initially placed by the consumer. |
| `order_status` | `VARCHAR` | **No** | Enum | Allowed: `PENDING`, `CONFIRMED`, `SHIPPED`, `DELIVERED`, `CANCELLED`, `REFUNDED`. |
| `payment_method` | `VARCHAR` | **No** | Enum | Allowed: `CREDIT_CARD`, `DEBIT_CARD`, `UPI`, `NET_BANKING`, `CRYPTO`. |
| `currency` | `VARCHAR` | **No** | 3-char code | Transaction ISO currency (`USD`). |
| `amount` | `DOUBLE` | **No** | `amount > 0.0` | Net order subtotal. Strictly greater than 0; rejects negative/zero amounts. |
| `tax_amount` | `DOUBLE` | **No** | `tax_amount >= 0.0` | **Derived field**: Standard statutory tax calculated as `ROUND(amount * 0.18, 2)`. |
| `total_amount` | `DOUBLE` | **No** | `total_amount >= amount` | **Derived field**: Total invoice amount calculated as `ROUND(amount + tax_amount, 2)`. |
| `items_count` | `INTEGER` | **No** | `items_count >= 1` | Total quantity of merchandise units in order. |
| `updated_at` | `TIMESTAMP` | **No** | ISO-8601 UTC | Event mutation watermark timestamp. Used to resolve CDC ordering & prevent stale overwrites. |
| `is_deleted` | `BOOLEAN` | **No** | Default `FALSE` | Soft-delete tombstone indicator for audit preservation. |
| `run_id` | `VARCHAR` | **No** | UUID / String | **Lineage metadata**: Run identifier of the pipeline execution that published the row. |
| `source_file` | `VARCHAR` | **No** | File path / Stream name | **Lineage metadata**: Provenance indicating the origin batch or raw source file. |
| `ingested_at` | `TIMESTAMP` | **No** | ISO-8601 UTC | **System metadata**: Host timestamp when row passed through the validation engine. |

---

### 2.3 Quarantine Sink: `quarantine_run_<run_id>`
Captures all rejected records that fail schema validation, domain constraints, or referential integrity.

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `record_index` | `BIGINT` | Position index of the failed row within the incoming batch. |
| `raw_payload` | `VARCHAR` | Verbatim unparsed string or JSON payload of the invalid record. |
| `rejection_rule` | `VARCHAR` | Category code: `MALFORMED_JSON`, `SCHEMA_OR_DOMAIN_CONSTRAINT`, `REFERENTIAL_INTEGRITY_VIOLATION`, `INTRA_BATCH_SHADOWED_DUPLICATE`. |
| `rejection_reason`| `VARCHAR` | Detailed diagnostic message explaining why validation rejected the record. |
| `rejected_at` | `VARCHAR` | ISO-8601 timestamp when the rejection occurred. |
| `run_id` | `VARCHAR` | Ingestion run identifier associated with the batch. |
| `source_file` | `VARCHAR` | Origin file or batch sink where the rejected payload arrived from. |

---

### 2.4 Audit & Observability: `pipeline_audit_log`
Tracks execution lineage, performance metrics, and operational health.

| Column Name | SQL Type | Description |
| :--- | :--- | :--- |
| `run_id` | `VARCHAR PRIMARY KEY` | Unique execution UUID. |
| `pipeline_strategy` | `VARCHAR` | Ingestion strategy: `FullRefreshPipeline`, `IncrementalMergePipeline`, `PartitionReplacementPipeline`. |
| `batch_id` | `VARCHAR` | Identifier of the ingested batch (e.g. `batch_001_baseline`). |
| `status` | `VARCHAR` | Run status: `SUCCESS`, `FAILED`. |
| `start_time` | `TIMESTAMP` | UTC start timestamp of execution. |
| `end_time` | `TIMESTAMP` | UTC completion timestamp. |
| `duration_seconds` | `DOUBLE` | Wall-clock execution time in seconds. |
| `input_records` | `INTEGER` | Total raw records received in the batch. |
| `valid_records` | `INTEGER` | Count of records successfully passing validation. |
| `rejected_records` | `INTEGER` | Count of records routed to quarantine. |
| `inserted_records` | `INTEGER` | Count of new keys inserted into target table. |
| `updated_records` | `INTEGER` | Count of existing keys updated in target table. |
| `deleted_records` | `INTEGER` | Count of records deleted / soft-deleted. |
| `target_total_records`| `INTEGER` | Final total count of rows in target table after commit. |
| `error_message` | `VARCHAR` | Exception trace / error reason if `status = 'FAILED'`. |
