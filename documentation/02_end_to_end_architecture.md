# End-to-End Architecture (Glue Edition)

## High-Level Pipeline Flow (Conceptual Overview)

This Glue Edition architecture processes the complete e-commerce dataset through a multi-zone data lake and analytics pipeline, 
with clear separation between ingestion, transformation, modeling, and serving layers:

RAW (S3)
↓ (Event-driven ingestion via Airflow)
BRONZE (S3 – validated raw files)
↓ (Glue Spark: Bronze → Silver)
SILVER (S3 – cleaned, normalized, DQ-enforced)
↓ (Glue Spark: Silver → Gold)
GOLD (S3 – dimensional, fact, and mart tables)
↓ (Airflow-triggered COPY)
REDSHIFT SERVERLESS (Analytics & BI layer)


- Airflow controls orchestration, dependency ordering, and validation.
- AWS Glue Spark jobs perform all heavy data transformation and modeling.
- Amazon Redshift Serverless acts as the final serving layer for analytics and BI.

---

## Architecture Diagram (Mermaid)

```mermaid
flowchart TD

subgraph S3_RAW["S3 RAW Zone"]
    R1["events/*"]
    R2["users/*"]
    R3["transactions/*"]
end

subgraph Airflow["Airflow Control Plane"]
    A1["Event-driven RAW → BRONZE DAGs"]
    A2["Master Orchestrator DAG"]
    A3["Trigger Bronze → Silver (Glue)"]
    A4["Trigger Silver → Gold (Glue)"]
    A5["Trigger Gold → Redshift Loader"]
end

subgraph S3_BRONZE["S3 BRONZE Zone"]
    B1["events/"]
    B2["users/"]
    B3["transactions/"]
end

subgraph Glue_B2S["Glue Job: Bronze → Silver"]
    G1["Schema normalization<br/>Cleaning<br/>Dedupe<br/>Row quarantine<br/>SCD-style user handling"]
end

subgraph S3_SILVER["S3 SILVER Zone"]
    S1["events/"]
    S2["users/"]
    S3["transactions/"]
end

subgraph Glue_S2G["Glue Job: Silver → Gold"]
    G2["Dim tables<br/>Fact tables<br/>Mart tables"]
end

subgraph S3_GOLD["S3 GOLD Zone"]
    Gd["DIM: users, date, country, status"]
    Gf["FACT: events, transactions, user_activity"]
    Gm["MART: revenue, DAU, LTV"]
end

subgraph REDSHIFT["Redshift Serverless"]
    RS["COPY Parquet → Analytics Tables"]
end

R1 --> A1
R2 --> A1
R3 --> A1

A1 --> B1
A1 --> B2
A1 --> B3

A2 --> A3 --> Glue_B2S
Glue_B2S --> S1
Glue_B2S --> S2
Glue_B2S --> S3

A2 --> A4 --> Glue_S2G
Glue_S2G --> Gd
Glue_S2G --> Gf
Glue_S2G --> Gm

A2 --> A5 --> RS
Gd --> RS
Gf --> RS
Gm --> RS


## Manual Architecture Explanation

### Layer 1 — RAW Zone (Amazon S3)

- Stores 9.1 GB of real-world, messy source data
- Mixed formats: CSV, JSON, NDJSON, TXT, GZ, Parquet
- No schema enforcement and no validation
- Files arrive asynchronously from upstream systems
- Airflow S3 sensors detect new file arrivals

**Purpose:** act as an immutable landing zone for all upstream data

---

### Layer 2 — BRONZE Zone (Amazon S3)

Produced by event-driven Airflow ingestion DAGs.

Key characteristics:
- File-level validation only
- Supported format checks
- Readability verification
- Invalid or corrupt files moved to quarantine
- Valid files copied 1:1 into structured dataset prefixes
- No schema enforcement and no transformation

**Purpose:** ingestion integrity and isolation of bad files

---

### Layer 3 — SILVER Zone (Amazon S3)

Produced by Glue Spark Job: **Bronze → Silver**

Key responsibilities:
- Schema normalization and schema-drift handling
- Canonical column typing
- Null standardization and cleanup
- Complex timestamp parsing
- Dataset-specific deduplication
- Row-level quarantine for invalid records
- SCD-style user history handling
- Centralized data quality validation
- Strict `event_date` partition enforcement
- Metrics written to S3 for observability

**Purpose:** clean, trustworthy, analytics-ready base tables

---

### Layer 4 — GOLD Zone (Amazon S3)

Produced by Glue Spark Job: **Silver → Gold**

Data modeling layer:

- **Dimension tables**
  - `dim_users`
  - `dim_date`
  - `dim_country`
  - `dim_status`

- **Fact tables**
  - `fact_events`
  - `fact_transactions`
  - `fact_user_activity`

- **Mart tables**
  - `daily_revenue`
  - `daily_active_users`
  - `user_ltv`

Additional characteristics:
- Stable schemas enforced via explicit `StructType` definitions
- Broadcast joins for enrichment
- Business logic applied at scale
- Deterministic full refresh (overwrite per run)

**Purpose:** BI-ready dimensional and analytical datasets

---

### Layer 5 — Redshift Serverless (Warehouse / Serving Layer)

Loaded by Airflow Redshift Loader DAG.

Key responsibilities:
- Tables created via Redshift Data API
- Gold Parquet data ingested using `COPY`
- IAM-role-based secure access
- Post-load row-count validation
- Serves dashboards, ad-hoc SQL, and analytics workloads

**Purpose:** low-latency analytics and BI consumption
