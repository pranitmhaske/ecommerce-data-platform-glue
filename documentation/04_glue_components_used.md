# Glue Components Used 

This section documents all AWS Glue–related components actually used in the pipeline and why they were chosen.

---

## Glue Components Used (Glue Edition)

The pipeline uses:
- Two AWS Glue Spark jobs
- Minimal IAM roles
- Job parameters injected by Airflow
- Metrics written to S3
- Lifecycle policies documented as recommended governance

---

## Glue Jobs

### Glue Job — Bronze → Silver (bronze_to_silver_transformation.py)

**Purpose**
- Clean highly inconsistent 9.1 GB raw data
- Normalize schemas and handle schema drift
- Sanitize nulls, void columns, and invalid types
- Dataset-specific deduplication
- Row-level quarantine for invalid records
- Enforce strict event_date partitioning
- Generate data quality metrics

**Key Features**
- Handles mixed formats: JSON, NDJSON, CSV, CSV.GZ, TXT, Parquet
- Canonical schema enforcement using code-based schemas
- SCD-style user history handling in Silver
- Centralized row-level quarantine logic
- Fail-fast checks for empty outputs
- Writes fully partitioned Silver datasets

**Job Parameters (passed from Airflow)**

| Parameter | Description |
|---------|------------|
| `--bronze_path` | Bronze input base path |
| `--silver_path` | Silver output base path |
| `--quarantine_path` | Quarantine S3 prefix |
| `--metrics_path` | Metrics output location |
| `--JOB_NAME` | Glue job name |

---

### Glue Job — Silver → Gold (gold_dim_fact_mart.py)

**Purpose**
- Build analytics-ready dimension, fact, and mart tables
- Apply business logic at scale
- Enforce schema stability for BI consumption

**Key Features**
- Broadcast joins for enrichment
- Strict schemas via explicit `StructType`
- Deterministic full refresh (overwrite) modeling
- Timestamp safety and null handling
- Produces:
  - Dimensions: users, date, country, status
  - Facts: events, transactions, user_activity
  - Marts: daily_revenue, daily_active_users, user_ltv

**Job Parameters**

| Parameter | Description |
|---------|------------|
| `--SILVER_BASE` | Silver input |
| `--GOLD_DIM` | Dimension output path |
| `--GOLD_FACT` | Fact output path |
| `--GOLD_MART` | Mart output path |

---

## Glue worker type & Runtime

**worker type**
- G.1X (default)

**Why this is correct**
- 9.1 GB dataset
- CPU and I/O heavy workloads (schema drift, joins, aggregations)
- Cost-efficient for repeated runs
- Easily scalable to G.2X or G.4X as data volume grows

Jobs run as distributed Spark jobs, not Python Shell jobs.

---

## IAM Roles Used (Least-Privilege Design)

### IAM Role — GlueExecutionRole

Used by both Glue Spark jobs.

**Permissions**
- `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on:
  - ecom-p3-bronze
  - ecom-p3-silver
  - ecom-p3-gold
  - ecom-p3-quarantine
  - ecom-p3-metrics
  - ecom-p3-scripts
- CloudWatch Logs (Glue job logs)
- Glue service execution permissions
- `kms:Decrypt` (only if S3 encryption is enabled)

### IAM Role — RedshiftCopyRole

Used by Redshift Serverless COPY commands.

- Read access to `s3://ecom-p3-gold`
- Trust relationship: `redshift-serverless.amazonaws.com`

**Why this matters**
- Least privilege access
- Clear blast-radius control
- Matches real enterprise security posture

---

## Glue Metrics & Logging

**Metrics**

Written to:
- `s3://ecom-p3-metrics/{dataset}/`

Includes:
- Total row counts
- Null rates per column
- Data quality rule results
- Quarantined row counts

**Logging**

Glue job logs emitted to CloudWatch:
- `/aws-glue/jobs/output`
- `/aws-glue/jobs/error`
- `/aws-glue/jobs/logs-v2`

**Purpose:** auditability, debugging, and governance

---

## Bad Data Handling

**Important correction**
- Glue native `badRecordsPath` is not used
- Spark permissive parsing is not relied upon

**Actual implementation**
- File-level corruption  
  - Handled earlier in RAW → BRONZE Airflow ingestion
- Row-level corruption  
  - Explicitly handled in Glue code using:
    - Schema validation
    - Data quality rules
    - Null checks
    - Type enforcement
    - Business rule validation

Invalid rows are written to:
- `s3://ecom-p3-quarantine/{dataset}/`

**Why**
- Deterministic behavior
- Full control over error classification
- No silent data loss
- Easier auditing than Glue-managed corruption handling

This is a custom quarantine pattern, not Glue-managed corruption handling.

---

## S3 Lifecycle Policies (Documented for Governance)

Lifecycle rules are recommended, not required, and are documented for completeness.

| Layer | Retention | Reason |
|-----|-----------|--------|
| RAW | 60–90 days | Replay and audit |
| BRONZE | 30–120 days | Temporary staging |
| SILVER | No expiry | Reprocessing and backfills |
| GOLD | No expiry | Business-critical |
| QUARANTINE | 30 days | Debug only |
| METRICS | 180 days | Trend analysis |

**Optional transitions**
- STANDARD → STANDARD_IA
- Optional GLACIER_IR for older RAW and BRONZE data

Lifecycle policies improve cost efficiency without changing architecture.
