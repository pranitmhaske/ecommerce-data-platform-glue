import json
import logging
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

# ============================================================
# CONFIG
# ============================================================

REGION = "ap-south-1"

SILVER_BASE = "s3://ecom-p3-silver"
GOLD_DIM_BASE = "s3://ecom-p3-gold/dim"
GOLD_FACT_BASE = "s3://ecom-p3-gold/fact"
GOLD_MART_BASE = "s3://ecom-p3-gold/mart"

SCRIPTS_ZIP = "s3://ecom-p3-scripts/spark_jobs.zip"
SCRIPT_PATH = "s3://ecom-p3-scripts/scripts/gold_dim_fact_mart.py"

default_args = {
    "owner": "pranit",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)


def _get_s3_client():
    return boto3.client("s3", region_name=REGION)


def on_failure_callback(context):
    ti = context.get("task_instance")
    dag_run = context.get("dag_run")
    log.error(
        f"[GLUE_S2G] Task failed: {ti.task_id} | DAG: {dag_run.dag_id if dag_run else 'unknown'}"
    )


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="glue_silver_to_gold_spark",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=on_failure_callback,
    description="Glue Spark Silver→Gold (dim/fact/mart) pipeline",
) as dag:

    # ---------------------- 1. Sanity / config check ---------------------- #
    @task()
    def sanity_check_config():
        glue_job_name = Variable.get("GLUE_JOB_S2G_NAME", default_var=None)
        if not glue_job_name:
            raise AirflowFailException(
                "Airflow Variable 'GLUE_JOB_S2G_NAME' not set. Create Glue job and store name first."
            )

        s3 = _get_s3_client()

        def _check_prefix(url: str):
            if not url.startswith("s3://"):
                raise AirflowFailException(f"Invalid S3 URL: {url}")
            bucket, *key_parts = url[5:].split("/", 1)
            s3.head_bucket(Bucket=bucket)
            return url

        checked = {
            "silver": _check_prefix(SILVER_BASE),
            "gold_dim": _check_prefix(GOLD_DIM_BASE),
            "gold_fact": _check_prefix(GOLD_FACT_BASE),
            "gold_mart": _check_prefix(GOLD_MART_BASE),
        }

        log.info("[GLUE_S2G] Config OK. GlueJob=%s, Checked=%s", glue_job_name, checked)
        return {"glue_job": glue_job_name, "layout": checked}

    # ---------------------- 2. Run Glue Spark job ---------------------- #
    glue_step = GlueJobOperator(
        task_id="silver_to_gold_glue_job",
        job_name="{{ task_instance.xcom_pull(task_ids='sanity_check_config')['glue_job'] }}",
        script_location=SCRIPT_PATH,
        region_name=REGION,
        aws_conn_id="aws_default",
        wait_for_completion=True,
        retries=0,
        script_args={
            "--SILVER_BASE": SILVER_BASE,
            "--GOLD_DIM": GOLD_DIM_BASE,
            "--GOLD_FACT": GOLD_FACT_BASE,
            "--GOLD_MART": GOLD_MART_BASE,
        },
    )

    # ---------------------- 3. Verify Gold outputs exist ---------------------- #
    @task()
    def verify_gold_outputs():
        s3 = _get_s3_client()

        def _split_s3(url: str):
            bucket, *key_parts = url[5:].split("/", 1)
            prefix = key_parts[0] if key_parts else ""
            return bucket, prefix.rstrip("/")

        checks = {
            "dim_users": GOLD_DIM_BASE + "/dim_users",
            "fact_events": GOLD_FACT_BASE + "/fact_events",
            "fact_transactions": GOLD_FACT_BASE + "/fact_transactions",
            "daily_revenue": GOLD_MART_BASE + "/daily_revenue",
        }

        stats = {}
        missing = []

        for name, url in checks.items():
            bucket, prefix = _split_s3(url)
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
            count = resp.get("KeyCount", 0)
            stats[name] = int(count)
            if count == 0:
                missing.append(name)

        log.info("[GLUE_S2G] Gold S3 verification stats: %s", json.dumps(stats))

        if missing:
            raise AirflowFailException(
                f"Glue job finished but gold has no objects for: {missing}"
            )

        return {"verified": True, "stats": stats}

    cfg = sanity_check_config()
    done = glue_step
    verification = verify_gold_outputs()

    cfg >> done >> verification
