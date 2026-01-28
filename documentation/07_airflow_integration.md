## ✅ Section 7 — Integration with Airflow (Enterprise-Level Orchestration)

This project runs as a fully automated, event-driven, end-to-end data pipeline.  
Once deployed, the Glue Bronze → Silver → Gold → Redshift transformations execute without any manual intervention. Airflow continuously monitors RAW data arrivals, orchestrates Glue jobs, validates configuration, and enforces strict sequencing across all layers.

This orchestration layer is what elevates the project from isolated Glue jobs to a real-world production data platform.

---

### 7.1 Airflow’s Role in the Architecture
Airflow acts as the central control plane for the pipeline and orchestrates:

**1️ RAW → BRONZE (Event-Driven Ingestion)**
- S3 sensors detect incoming RAW files
- Valid formats are copied to BRONZE
- Invalid or unreadable files are moved to QUARANTINE
- RAW copies are removed after successful ingestion

**2️ BRONZE → SILVER (Glue Spark Job)**
- Triggered via TriggerDagRunOperator
- Airflow validates configuration before execution
- Glue performs cleaning, normalization, deduplication, quarantine, and partitioning

**3️ SILVER → GOLD (Glue Spark Job)**
- Triggered only after Silver completes successfully
- Airflow enforces strict dependency ordering

**4️ GOLD → Redshift Load**
- Executed only after Gold datasets are verified
- Airflow loads analytics tables using Redshift Serverless

**5️ Master Orchestration**
- A single master DAG coordinates all transformation stages:
  BRONZE → SILVER → GOLD → REDSHIFT

This mirrors how enterprise pipelines are structured in production environments.

---

### 7.2 Event-Driven RAW → BRONZE Design
RAW ingestion uses AWS-native, event-driven orchestration:

S3KeySensor(
    bucket_name="ecom-p3-raw",
    bucket_key="transactions/*"
)

As soon as any supported file lands in RAW (CSV, JSON, NDJSON, TXT, GZ, Parquet), the DAG automatically:
- Validates file format
- Copies valid files to BRONZE
- Routes corrupt or invalid files to QUARANTINE
- Deletes the RAW copy

**Enterprise benefits**
- Near-real-time ingestion
- Zero manual triggering
- Transparent error isolation
- Multi-format support by design

---

### 7.3 Glue Job Triggering via Airflow
Each Glue transformation is triggered using:

TriggerDagRunOperator(
    trigger_dag_id="glue_bronze_to_silver_spark"
)

Glue jobs receive:
- S3 input/output paths
- Metrics and quarantine locations
- Glue job names
- Runtime configuration

All parameters are externalized and injected by Airflow, which is standard enterprise practice.

---

### 7.4 Configuration Validation & XCom Usage
Before triggering Glue jobs, Airflow validates required configuration:

glue_job_name = Variable.get("GLUE_JOB_S2G_NAME")

The validated value is passed via XCom into the Glue operator.

This ensures:
- Missing configuration fails early
- Incorrect job names never run
- Airflow acts as a centralized configuration authority

---

### 7.5 Fail-Safe Orchestration Guarantees
Your DAGs enforce production-grade safety controls:
- wait_for_completion = True
- Explicit on_failure_callback
- Retry policies
- max_active_runs = 1 (no parallel state corruption)

If any stage fails, the pipeline halts immediately, preventing partial or corrupted downstream data.

---

### 7.6 Master Pipeline DAG
The master orchestrator enforces strict sequencing:

trigger_bronze_to_silver  
↓  
trigger_silver_to_gold  
↓  
trigger_gold_to_redshift  

This guarantees:
- No race conditions
- No orphaned states
- Deterministic data lineage
- Predictable recovery behavior

---

### 7.7 Why This Airflow Design Is Enterprise-Level
Your orchestration includes all patterns expected in senior-level data platforms:
- Fully automated, event-driven execution
- Multi-format ingestion
- Glue Spark integration
- Strict dependency enforcement
- XCom & Variable-driven configuration
- Fail-fast behavior
- Master orchestration
- End-to-end lineage: RAW → BRONZE → SILVER → GOLD → REDSHIFT
