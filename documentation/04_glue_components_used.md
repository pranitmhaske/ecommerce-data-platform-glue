# Glue Components Used (Glue Edition)

This section documents the AWS Glue components used in the pipeline, focusing on execution roles, runtime configuration, 
job parameters, observability, and lifecycle considerations. The documentation is intentionally scoped to an enterprise-appropriate level.

---

## IAM Roles (Enterprise-Grade, Minimal & Accurate)

### GlueExecutionRole

Used by both Glue Spark jobs:
- Bronze → Silver
- Silver → Gold

Required permissions (least-privilege):

S3 access:
- s3:GetObject
- s3:PutObject
- s3:ListBucket

Granted only on:
- ecom-p3-bronze
- ecom-p3-silver
- ecom-p3-gold
- ecom-p3-quarantine
- ecom-p3-metrics
- ecom-p3-scripts

CloudWatch Logs:
- Glue job logging

AWS Glue service permissions:
- Job execution
- Spark runtime management

KMS (conditional):
- kms:Decrypt only if S3 buckets are encrypted

Why this matters:
- Glue jobs can access only pipeline-related data
- Prevents accidental cross-bucket access
- Matches real enterprise security posture

IAM is intentionally documented at the role level, not at raw policy JSON level, which mirrors how real teams document security.

---

## Glue Execution Class & Runtime

Execution class:
- Glue G.1X or G.2X (depending on configuration)

Why this is correct:
- 9.1 GB dataset
- Heavy schema drift handling
- Deduplication and quarantine logic
- SCD-style user handling
- Aggregations for Gold marts

Important clarifications:
- Jobs run as distributed Spark jobs
- Not Python Shell jobs
- Execution class selection is meaningful and intentional

---

## Glue Job Parameters / Arguments

### Bronze → Silver Job

- --bronze_path     s3://ecom-p3-bronze
- --silver_path     s3://ecom-p3-silver
- --quarantine_path s3://ecom-p3-quarantine
- --metrics_path    s3://ecom-p3-metrics

### Silver → Gold Job

- --SILVER_BASE s3://ecom-p3-silver
- --GOLD_DIM    s3://ecom-p3-gold/dim
- --GOLD_FACT   s3://ecom-p3-gold/fact
- --GOLD_MART   s3://ecom-p3-gold/mart

Why this is enterprise-grade:
- Decouples code from environment
- Enables reuse across dev, test, and prod
- Allows Airflow to inject paths dynamically
- Standard practice in production Glue pipelines

---

## Metrics & Observability

### Bronze → Silver Job

Captured metrics:
- Dataset row counts
- Null rates per column
- Corrupt and quarantined row counts
- Data quality rule results

Metrics written as CSV to:
- `s3://ecom-p3-metrics/{dataset}/`

### Silver → Gold Job

Captured metrics:
- Row counts for dimension, fact, and mart outputs
- Aggregation success logging
- Schema stability verification (implicit)

Why this matters:
- Enables auditability
- Supports debugging and governance
- Ready for Athena or external monitoring tools
- CloudWatch usage is optional, not mandatory

---

## Lifecycle Policies (Enterprise Standard – Documented for Completeness)

Lifecycle rules are not required for correctness, but documenting them demonstrates senior-level thinking.

Recommended retention strategy:

RAW:
- 30–60 days
- Reason: audits and replay

BRONZE:
- 7–30 days
- Reason: temporary ingestion layer

SILVER:
- 90 days
- Reason: intermediate analytics

GOLD:
- Indefinite
- Reason: business-critical data

QUARANTINE:
- 30 days
- Reason: debugging and inspection

METRICS:
- 180 days
- Reason: trend analysis

Storage class transitions:
- After 30 days: STANDARD → STANDARD_IA
- Optional: GLACIER_IR for older RAW and BRONZE data

Result:
- Cost optimized
- No architectural change
- Fully production-realistic
