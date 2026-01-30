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

BRONZE_BASE = "s3://ecom-p3-bronze"
SILVER_BASE = "s3://ecom-p3-silver"
QUARANTINE_BASE = "s3://ecom-p3-quarantine"
METRICS_BASE = "s3://ecom-p3-metrics"
SCRIPTS_ZIP = "s3://ecom-p3-scripts/spark_jobs.zip"
SCRIPT_PATH = "s3://ecom-p3-scripts/bronze_to_silver_transformation.py"

DOMAINS = ["events", "users", "transactions"]

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
        f"[GLUE_B2S] Task failed: {ti.task_id} | DAG: {dag_run.dag_id if dag_run else 'unknown'}"
    )


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="glue_bronze_to_silver_spark",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=on_failure_callback,
    description="Glue Spark Bronze→Silver pipeline",
) as dag:

    # ---------------------- 1. Sanity / config check ---------------------- #
    @task()
    def sanity_check_config():
        glue_job_name = Variable.get("GLUE_JOB_B2S_NAME", default_var=None)
        if not glue_job_name:
            raise AirflowFailException(
                "Airflow Variable 'GLUE_JOB_B2S_NAME' not set. "
                "Create Glue job manually first and store the job name."
            )

        s3 = _get_s3_client()

        def _check_prefix(url: str):
            if not url.startswith("s3://"):
                raise AirflowFailException(f"Invalid S3 URL: {url}")
            bucket, *key_parts = url[5:].split("/", 1)
            s3.head_bucket(Bucket=bucket)
            return url

        checked = {
            "bronze": _check_prefix(BRONZE_BASE),
            "silver": _check_prefix(SILVER_BASE),
            "quarantine": _check_prefix(QUARANTINE_BASE),
            "metrics": _check_prefix(METRICS_BASE),
        }

        log.info("[GLUE_B2S] Config sanity OK. GlueJob=%s, Checked=%s", glue_job_name, checked)
        return {"glue_job": glue_job_name, "layout": checked}

    # ---------------------- 2. Run Glue Spark job ---------------------- #

    glue_step = GlueJobOperator(
        task_id="bronze_to_silver_glue_job",
        job_name="{{ task_instance.xcom_pull(task_ids='sanity_check_config')['glue_job'] }}",
        script_location=SCRIPT_PATH,
        region_name=REGION,
        script_args={
            "--bronze_path": BRONZE_BASE,
            "--silver_path": SILVER_BASE,
            "--quarantine_path": QUARANTINE_BASE,
            "--metrics_path": METRICS_BASE,
            "--extra-py-files":SCRIPTS_ZIP,
        },
        aws_conn_id="aws_default",
        wait_for_completion=True,
        retries=0,
    )

    # ---------------------- 3. Verify Silver outputs exist ---------------------- #
    @task()
    def verify_silver_outputs():
        s3 = _get_s3_client()

        def _split_s3(url: str):
            bucket, *key_parts = url[5:].split("/", 1)
            prefix = key_parts[0] if key_parts else ""
            return bucket, prefix.rstrip("/")

        bucket, silver_prefix = _split_s3(SILVER_BASE)

        missing = []
        stats = {}

        for dom in DOMAINS:
            import json
import logging
import time
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

BRONZE_BASE = "s3://ecom-p3-bronze"
SILVER_BASE = "s3://ecom-p3-silver"
QUARANTINE_BASE = "s3://ecom-p3-quarantine"
METRICS_BASE = "s3://ecom-p3-metrics"
SCRIPTS_ZIP = "s3://ecom-p3-scripts/spark_jobs.zip"
SCRIPT_PATH = "s3://ecom-p3-scripts/bronze_to_silver_transformation.py"

DOMAINS = ["events", "users", "transactions"]

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
        f"[GLUE_B2S] Task failed: {ti.task_id} | DAG: {dag_run.dag_id if dag_run else 'unknown'}"
    )


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="glue_bronze_to_silver_spark",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=on_failure_callback,
    description="Glue Spark Bronze→Silver pipeline",
) as dag:

    # ---------------------- 1. Sanity / config check ---------------------- #
    @task()
    def sanity_check_config():
        glue_job_name = Variable.get("GLUE_JOB_B2S_NAME", default_var=None)
        if not glue_job_name:
            raise AirflowFailException(
                "Airflow Variable 'GLUE_JOB_B2S_NAME' not set. "
                "Create Glue job manually first and store the job name."
            )

        s3 = _get_s3_client()

        def _check_prefix(url: str):
            if not url.startswith("s3://"):
                raise AirflowFailException(f"Invalid S3 URL: {url}")
            bucket, *key_parts = url[5:].split("/", 1)
            s3.head_bucket(Bucket=bucket)
            return url

        checked = {
            "bronze": _check_prefix(BRONZE_BASE),
            "silver": _check_prefix(SILVER_BASE),
            "quarantine": _check_prefix(QUARANTINE_BASE),
            "metrics": _check_prefix(METRICS_BASE),
        }

        log.info("[GLUE_B2S] Config sanity OK. GlueJob=%s, Checked=%s", glue_job_name, checked)
        return {"glue_job": glue_job_name, "layout": checked}

    # ---------------------- 2. Run Glue Spark job ---------------------- #

    glue_step = GlueJobOperator(
        task_id="bronze_to_silver_glue_job",
        job_name="{{ task_instance.xcom_pull(task_ids='sanity_check_config')['glue_job'] }}",
        script_location=SCRIPT_PATH,
        region_name=REGION,
        script_args={
            "--bronze_path": BRONZE_BASE,
            "--silver_path": SILVER_BASE,
            "--quarantine_path": QUARANTINE_BASE,
            "--metrics_path": METRICS_BASE,
            "--extra-py-files":SCRIPTS_ZIP,
        },
        aws_conn_id="aws_default",
        wait_for_completion=True,
        retries=0,
    )

    # ---------------------- 3. Verify Silver outputs exist ---------------------- #
    @task()
    def verify_silver_outputs(prev: dict):
        s3 = _get_s3_client()

        def _split_s3(url: str):
            bucket, *key_parts = url[5:].split("/", 1)
            prefix = key_parts[0] if key_parts else ""
            return bucket, prefix.rstrip("/")

        bucket, silver_prefix = _split_s3(SILVER_BASE)

        missing = []
        stats = {}

        for dom in DOMAINS:
            dom_prefix = f"{silver_prefix}/{dom}/event_date="
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=dom_prefix, MaxKeys=1)
            count = resp.get("KeyCount", 0)
            stats[dom] = int(count)
            if count == 0:
                missing.append(dom)

        log.info("[GLUE_B2S] Silver S3 verification stats: %s", json.dumps(stats))

        if missing:
            raise AirflowFailException(
                f"Glue job finished but silver has no objects for domains: {missing}"
            )

        return {"verified": True, "stats": stats}

    cfg = sanity_check_config()
    done = glue_step
    verification = verify_silver_outputs(cfg)

    cfg >> done >> verification

            resp = s3.list_objects_v2(Bucket=bucket, Prefix=dom_prefix, MaxKeys=1)
            count = resp.get("KeyCount", 0)
            stats[dom] = int(count)
            if count == 0:
                missing.append(dom)

        log.info("[GLUE_B2S] Silver S3 verification stats: %s", json.dumps(stats))

        if missing:
            raise AirflowFailException(
                f"Glue job finished but silver has no objects for domains: {missing}"
            )

        return {"verified": True, "stats": stats}

    cfg = sanity_check_config()
    done = glue_step
    verification = verify_silver_outputs()

    cfg >> done >> verification
