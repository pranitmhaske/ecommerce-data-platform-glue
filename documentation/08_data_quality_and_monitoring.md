## Section 8 — Data Quality & Monitoring (DQ + Metrics)

Glue + Airflow pipeline implements data quality enforcement, ensuring that only trustworthy, schema-consistent, 
and business-valid data reaches the Silver and Gold layers. 

---

### 8.1 Multi-Layer Data Quality Architecture
Data quality is enforced at three distinct layers, each responsible for a different class of failures.

**1️ RAW → BRONZE (Airflow Ingestion Layer)**  
File-level validation only:
- File extension validation
- Readability checks (try_read)
- Automatic quarantine of:
  - Unreadable files
  - Invalid or unsupported formats
- RAW copies deleted only after successful ingestion

Purpose: Prevent corrupt files from ever entering Spark processing

---

**2️ BRONZE → SILVER (Glue Transformation Layer)**  
Row-level and schema-level enforcement:
- Schema normalization and drift handling
- Required-field validation
- Type-casting validation
- Deduplication correctness
- Row-level quarantine for invalid records
- Strict empty-output detection

Purpose: Guarantee Silver contains only clean, analyzable data

---

**3️ SILVER → GOLD (Glue Modeling Layer)**  
Business and analytical validation:
- Null safety for fact and mart measures
- Stable schema enforcement
- Aggregation correctness
- Controlled overwrites to prevent partial data exposure

Purpose: Ensure BI-ready analytical correctness

---

This layered design prevents bad data from propagating downstream, while still preserving invalid records for analysis.

---

### 8.2 Data Quality Utilities
The pipeline includes reusable, production-style data quality utilities.

**Core checks**
- dq_check_not_null(df, col)  
  Ensures critical identifiers and timestamps are non-null

Additional validation logic includes:
- Key presence validation
- Timestamp safety checks
- Dataset-specific rule enforcement

These utilities mirror patterns found in enterprise data governance frameworks, implemented directly in Spark.

---

### 8.3 validate() — Central Metrics Generator
Every Silver dataset passes through a centralized validation step.

**Metrics produced**
- Total row count
- Null rate per column
- Dataset-specific DQ checks (IDs, timestamps, amounts)
- Quarantined row counts (where applicable)

Metrics are written to S3 for each dataset, enabling:
- Debugging
- Auditing
- Trend analysis

---

### 8.4 S3-Based Metrics Storage
Metrics are written as CSV files to:
s3://ecom-p3-metrics/<dataset>/

Example structure:
ecom-p3-metrics/
- events/
- users/
- transactions/

Example metrics content:
metric,value  
total_rows,393220  
null_rate_age,0.0124  
null_rate_country,0.0001  
null_rate_ts,0.0008  

These files act as batch-level health indicators for every pipeline run.

---

### 8.5 Silver-Layer Strict Fail-Fast Policy
The Bronze → Silver Glue job enforces strict failure rules.

**STRICT_FAIL behavior**  
If any critical step produces zero valid rows, the job aborts immediately.

Examples:
- Empty DataFrame after schema normalization
- All rows quarantined
- Missing required identifiers (event_id, tx_id, user_id)
- Corrupt or unreadable partitions

This prevents:
- Silent data degradation
- Partial downstream updates
- BI exposure to incomplete data

---

### 8.6 Quarantine Strategy (DQ Failure Path)
Rows that fail data quality checks are isolated, not discarded.

**Quarantine locations**
- s3://ecom-p3-quarantine/events/
- s3://ecom-p3-quarantine/users/
- s3://ecom-p3-quarantine/transactions/

Why this matters:
- Silver and Gold contain only valid data
- Failed records remain available for forensic analysis
- Supports replay, fixes, and controlled reprocessing

This is a standard modern data platform pattern.

---

### 8.7 Monitoring via Logs & Metrics
Pipeline observability is achieved through:
- Airflow logs (ingestion, orchestration, failures)
- Glue job logs (Spark execution, strict-fail exceptions)
- S3 metrics outputs
- CloudWatch Glue logs

This provides end-to-end traceability and auditability.

---

### 8.8 Gold-Layer Data Quality Guarantees
Before Gold datasets are exposed to Redshift:
- Schemas are fully enforced
- Aggregations are recomputed deterministically
- Fact and mart tables are rebuilt in controlled overwrites
- Invalid or partial outputs are blocked by upstream failures

This ensures that BI consumers only see consistent, trusted datasets.
