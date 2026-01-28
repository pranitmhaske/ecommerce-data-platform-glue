# End-to-End Architecture (Glue Edition)

## High-Level Pipeline Flow

This Glue Edition architecture processes the complete e-commerce dataset through a multi-zone data lake and analytics pipeline with clear separation between ingestion, transformation, modeling, and serving layers.

RAW (Amazon S3)  
↓ (Event-driven ingestion via Airflow)  
BRONZE (Amazon S3 – validated raw files)  
↓ (AWS Glue Spark: Bronze → Silver)  
SILVER (Amazon S3 – cleaned, normalized, DQ-enforced)  
↓ (AWS Glue Spark: Silver → Gold)  
GOLD (Amazon S3 – dimensional, fact, and mart tables)  
↓ (Airflow-triggered COPY)  
REDSHIFT SERVERLESS (Analytics & BI layer)

**Core responsibilities**
- Airflow controls orchestration, dependency ordering, and validations.
- AWS Glue Spark jobs perform all large-scale transformations and data modeling.
- Amazon Redshift Serverless acts as the final analytics and BI serving layer.

---

## Architecture Explanation (Layer by Layer)

### Layer 1 — RAW Zone (Amazon S3)

**Characteristics**
- Stores ~9.1 GB of real-world, messy source data
- Mixed formats: CSV, JSON, NDJSON, TXT, GZ, Parquet
- No schema enforcement
- No validation
- Files arrive asynchronously from upstream systems
- Airflow S3 sensors detect new file arrivals

**Purpose**  
Immutable landing zone for all upstream data without assumptions or transformations.

---

### Layer 2 — BRONZE Zone (Amazon S3)

Produced by event-driven Airflow ingestion DAGs.

**Key characteristics**
- File-level validation only
- Supported format checks
- Readability verification
- Invalid or corrupt files moved to quarantine
- Valid files copied 1:1 into structured dataset prefixes
- No schema enforcement
- No transformations

**Purpose**  
Ensure ingestion integrity and isolate bad files early without altering valid data.

---

### Layer 3 — SILVER Zone (Amazon S3)

Produced by AWS Glue Spark job: **Bronze → Silver**

**Key responsibilities**
- Schema normalization and schema-drift handling
- Canonical column typing
- Null standardization and cleanup
- Complex timestamp parsing
- Dataset-specific deduplication
- Row-level quarantine for invalid records
- SCD-style user history handling
- Centralized data quality validation
- Strict `event_date` partition enforcement
- Data quality metrics written to S3 for observability

**Purpose**  
Create clean, trustworthy, analytics-ready base datasets.

---

### Layer 4 — GOLD Zone (Amazon S3)

Produced by AWS Glue Spark job: **Silver → Gold**

**Data modeling layer**

**Dimension tables**
- `dim_users`
- `dim_date`
- `dim_country`
- `dim_status`

**Fact tables**
- `fact_events`
- `fact_transactions`
- `fact_user_activity`

**Mart tables**
- `daily_revenue`
- `daily_active_users`
- `user_ltv`

**Additional characteristics**
- Stable schemas enforced using explicit `StructType`
- Broadcast joins for dimension enrichment
- Business logic applied at scale
- Deterministic full refresh (overwrite per run)

**Purpose**  
Produce BI-ready dimensional and analytical datasets.

---

### Layer 5 — Redshift Serverless (Serving Layer)

Loaded via Airflow Redshift Loader DAG.

**Key responsibilities**
- Tables created using Redshift Data API
- Gold Parquet data ingested via `COPY`
- IAM role-based secure access
- Post-load row-count validation
- Serves dashboards, ad-hoc SQL, and analytics workloads

**Purpose**  
Low-latency analytics and BI consumption.
