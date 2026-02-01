# Data Lake Layout (AWS S3 — Glue )

This Glue pipeline applies a multi-zone AWS S3 data lake architecture aligned with lakehouse patterns.
The design emphasizes data immutability, isolation of bad data, schema stability, and analytics readiness.

---

## Bucket Overview

All data processing is organized into purpose-specific S3 buckets, each with a clearly defined responsibility.

---

## RAW Zone — s3://ecom-p3-raw
Source data landing zone (immutable)

- Stores files exactly as received
- Mixed, real-world formats: JSON, NDJSON, CSV, TXT, GZIP, Parquet
- No schema enforcement
- No validation
- No transformations
- Acts as the source of truth

    ecom-p3-raw/
      events/
      users/
      transactions/

---

## BRONZE Zone — s3://ecom-p3-bronze
Validated file-level ingestion output

Produced by event-driven Airflow ingestion DAGs.

- File format validation
- Readability checks
- Invalid or corrupt files moved to QUARANTINE
- No content modification
- No schema enforcement

    ecom-p3-bronze/
      events/
      users/
      transactions/

---

## SILVER Zone — s3://ecom-p3-silver
Cleaned, normalized, analytics-ready base tables

Produced by Glue Spark Job (Bronze → Silver).

- Schema normalization and drift handling
- Canonical typing and null standardization
- Dataset-specific deduplication
- Row-level quarantine for invalid records
- SCD-style user history handling
- Centralized data quality validation
- Partitioned by event_date

    ecom-p3-silver/
      events/event_date=YYYY-MM-DD/
      users/event_date=YYYY-MM-DD/
      transactions/event_date=YYYY-MM-DD/

---

## GOLD Zone — s3://ecom-p3-gold
Curated analytical data model

Produced by Glue Spark Job (Silver → Gold).

### Dimension Tables
    ecom-p3-gold/dim/
      dim_users/
      dim_country/
      dim_status/
      dim_date/

### Fact Tables
    ecom-p3-gold/fact/
      fact_events/
      fact_transactions/
      fact_user_activity/

### Mart Tables
    ecom-p3-gold/mart/
      daily_revenue/
      daily_active_users/
      user_ltv/

- Stable schemas enforced via explicit StructTypes
- Deterministic full-refresh (overwrite) modeling
- Optimized for Redshift ingestion and BI consumption

---

## QUARANTINE Zone — s3://ecom-p3-quarantine
Error isolation and forensic analysis

Stores:
- Corrupt records
- Schema-drifted rows
- Failed DQ records
- Invalid data types
- Impossible timestamps
- Missing primary keys

    ecom-p3-quarantine/
      events/
      users/
      transactions/

Purpose: prevent bad data from breaking pipelines or polluting analytics.

---

## METRICS Zone — s3://ecom-p3-metrics
Pipeline observability and governance

- Dataset-level CSV metrics emitted by Glue jobs
- Used for monitoring, debugging, and audit trails

    ecom-p3-metrics/
      events/
      users/
      transactions/

Each metrics file includes:
- Total row counts
- Null rates per column
- Data quality rule outcomes

---

## SCRIPT Bucket — s3://ecom-p3-scripts
Executable artifacts and Spark job bundles

    ecom-p3-scripts/
      spark_jobs.zip
      scripts/bronze_to_silver.py
      scripts/gold_dim_fact_mart.py

Storage location only — does not affect architectural design.

---

## Partitioning Strategy

Both SILVER and GOLD layers use date-based partitioning.

- Partition column: event_date
- Derived from: ts, updated_at, created_at

Why event_date:
- Consistent across datasets
- Business-time aligned
- Simplifies fact and mart aggregation
- Enables predicate pushdown (Athena / Redshift Spectrum)
- Avoids ingestion-time skew

Examples:

    s3://ecom-p3-silver/events/event_date=2024-10-05/part-*.parquet
    s3://ecom-p3-gold/fact/fact_events/event_date=2024-10-05/

---

## Layering Rationale

RAW — Immutable source of truth (Auditable, reproducible)  
BRONZE — File-level validation (Shields downstream systems)  
SILVER — Clean, schema-locked (Handles drift & inconsistency)  
GOLD — Analytics modeling (BI / warehouse friendly)  
QUARANTINE — Error isolation (Prevents data loss & failures)  
METRICS — Observability (Governance & trust)
