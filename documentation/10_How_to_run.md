## Section 10 — How to Run the Glue Version (Local + AWS)
Enterprise-grade, reproducible, and reviewer-friendly

This section explains exactly how the Glue version of the pipeline is executed, both for local validation and AWS production runs, 
without assumptions or hidden steps.

---

### 10.1 Overview
This project runs as a fully automated, Airflow-orchestrated Glue pipeline with the following execution flow:

RAW → BRONZE → SILVER → GOLD → REDSHIFT

- Ingestion: Event-driven via Airflow (S3 arrival)
- Transformations: AWS Glue Spark jobs
- Serving layer: Redshift Serverless
- Control plane: Airflow DAGs + Master Orchestrator

Once deployed, no manual intervention is required for production runs.

---

### 10.2 Local Dry-Run / Logic Validation (Developer Mode)
Local runs are used to validate logic, schema handling, and utilities before incurring AWS costs.  
They do not write to real S3 buckets.

**Prerequisites**
pip install pyspark==3.4.1 pandas pyarrow boto3 faker

**Local Test Execution (Bronze → Silver)**
python bronze_to_silver_transformation.py \
  --bronze_path ./include/bronze \
  --silver_path ./include/airflow_silver \
  --quarantine_path ./include/quarantine \
  --metrics_path ./include/metrics

**What Local Runs Validate**
- Multi-format readers (JSON / CSV / NDJSON / TXT / GZIP / Parquet)
- Schema drift handling
- Quarantine logic
- DQ rule enforcement
- Metrics generation
- Partition correctness

Local testing ensures Glue logic correctness without AWS cost.

---

### 10.3 Upload Glue Scripts to S3
All Glue scripts are stored centrally in the scripts bucket:

s3://ecom-p3-scripts/

Upload the packaged jobs:
aws s3 cp spark_jobs.zip s3://ecom-p3-scripts/

This ZIP contains:
- bronze_to_silver_transformation.py
- gold_dim_fact_mart.py
- all shared utils/

---

### 10.4 Create Glue Job — Bronze → Silver
**Glue Job Type:** Spark  

**Script location:**  
s3://ecom-p3-scripts/bronze_to_silver_transformation.py

**Job Arguments**
- --bronze_path      s3://ecom-p3-bronze
- --silver_path      s3://ecom-p3-silver
- --quarantine_path  s3://ecom-p3-quarantine
- --metrics_path     s3://ecom-p3-metrics

**Execution Notes**
- Execution Class: G.1X
- IAM Role: GlueJobRole
- Logs: CloudWatch
- Triggered only via Airflow, not manually

---

### 10.5 Create Glue Job — Silver → Gold
**Script location:**  
s3://ecom-p3-scripts/scripts/gold_dim_fact_mart.py

**Job Arguments**
- --SILVER_BASE   s3://ecom-p3-silver
- --GOLD_DIM      s3://ecom-p3-gold/dim
- --GOLD_FACT     s3://ecom-p3-gold/fact
- --GOLD_MART     s3://ecom-p3-gold/mart

**Output**
- Dimension tables
- Fact tables
- Mart tables
- Schema-locked Parquet datasets

---

### 10.6 Configure Airflow Integration
Your Airflow DAGs already handle Glue execution.

**Required Airflow Variables**  
Set once (already done in your project):
- GLUE_JOB_B2S_NAME = bronze_to_silver_glue
- GLUE_JOB_S2G_NAME = silver_to_gold_glue

Airflow:
- Validates variables at runtime
- Fails early if misconfigured
- Acts as the centralized control plane

---

### 10.7 Triggering the Pipeline

**Path A — Event-Driven (Production Mode)**
1. Upload files to:
   - s3://ecom-p3-raw/events/
   - s3://ecom-p3-raw/users/
   - s3://ecom-p3-raw/transactions/
2. Airflow ingestion DAG:
   - Detects new files
   - Validates formats
   - Moves data to BRONZE
   - Quarantines invalid files
3. Master orchestrator DAG triggers:
   Bronze → Silver → Gold → Redshift
4. Redshift tables are refreshed
5. BI systems can query immediately

This is the default enterprise execution path.

---

**Path B — Manual Trigger (Debug / Backfill Mode)**  
From Airflow UI, trigger:
- glue_bronze_to_silver_spark
- glue_silver_to_gold_spark
- glue_gold_to_redshift_serverless_loader

Used for:
- Debugging
- Controlled reprocessing
- Backfills

---

### 10.8 Validation After Completion
**Verify Silver**
aws s3 ls s3://ecom-p3-silver/events/

**Verify Gold**
aws s3 ls s3://ecom-p3-gold/dim/  
aws s3 ls s3://ecom-p3-gold/fact/  
aws s3 ls s3://ecom-p3-gold/mart/

**Verify Metrics**
aws s3 ls s3://ecom-p3-metrics/

**Verify Redshift**
SELECT COUNT(*) FROM ecommerce_gold.fact_events;  
SELECT * FROM ecommerce_gold.dim_users LIMIT 20;

---

### 10.9 Cost & Operational Notes
- Glue charges only while jobs are running
- No idle cost
- No always-on clusters
- Airflow controls execution frequency
- You already follow best practice:
  - create → run → destroy expensive infra (NAT / EMR when used)

This is cost-efficient and production-correct.
