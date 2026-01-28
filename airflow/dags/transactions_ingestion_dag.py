#transactions_ingestion_dag

import os, logging
import boto3
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# ===================== CONFIG ===================== #

RAW_BUCKET        = "ecom-p3-raw"
BRONZE_BUCKET     = "ecom-p3-bronze"
QUARANTINE_BUCKET = "ecom-p3-quarantine"

RAW_PREFIX    = "transactions/"
BRONZE_PREFIX = "transactions/"

s3 = boto3.client("s3", region_name="ap-south-1")
log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)


# ===================== HELPERS ===================== #

def list_raw_files():
    keys = []
    token = None
    while True:
        resp = (
            s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix=RAW_PREFIX, ContinuationToken=token)
            if token else
            s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix=RAW_PREFIX)
        )
        keys.extend([o["Key"] for o in resp.get("Contents", []) if not o["Key"].endswith("/")])
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys


def valid_format(key: str) -> bool:
    key = key.lower()

    # skip parquet metadata
    if any(x in key for x in ["_success", ".crc"]):
        return False

    # actual parquet part file
    if key.endswith(".parquet") and "part-" in key:
        return True

    return key.endswith((
        ".csv", ".csv.gz",
        ".json",
        ".ndjson", ".ndjson.gz",
        ".txt",
        ".gz",
    ))


def try_read(bucket, key):
    s3.get_object(Bucket=bucket, Key=key)
    return True


# ===================== EVENT-DRIVEN DAG ===================== #

default_args = {"owner": "pranit", "retries": 1}

with DAG(
    dag_id="raw_to_bronze_transactions_event_driven",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,     # event-driven
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    description="Event-driven RAW → BRONZE for transactions",
):

    # EVENT TRIGGER — waits for ANY file in RAW/transactions/*
    wait_for_raw_file = S3KeySensor(
        task_id="wait_for_raw_transaction_file",
        bucket_key=f"{RAW_PREFIX}*",
        bucket_name=RAW_BUCKET,
        wildcard_match=True,
        poke_interval=10,        # check every 10 sec
        timeout=60 * 60 * 24,    # 24 hours
    )

    @task()
    def move_files():
        files = list_raw_files()
        if not files:
            log.info("No RAW transaction files.")
            return

        for key in files:

            if not valid_format(key):
                s3.copy_object(
                    Bucket=QUARANTINE_BUCKET,
                    CopySource={"Bucket": RAW_BUCKET, "Key": key},
                    Key=key,
                )
                s3.delete_object(Bucket=RAW_BUCKET, Key=key)
                log.info(f"INVALID FORMAT → QUARANTINE: {key}")
                continue

            try:
                try_read(RAW_BUCKET, key)
            except Exception:
                s3.copy_object(
                    Bucket=QUARANTINE_BUCKET,
                    CopySource={"Bucket": RAW_BUCKET, "Key": key},
                    Key=key,
                )
                s3.delete_object(Bucket=RAW_BUCKET, Key=key)
                log.info(f"CORRUPT → QUARANTINE: {key}")
                continue

            fname = os.path.basename(key)
            bronze_key = f"{BRONZE_PREFIX}{fname}"

            s3.copy_object(
                Bucket=BRONZE_BUCKET,
                CopySource={"Bucket": RAW_BUCKET, "Key": key},
                Key=bronze_key,
            )
            s3.delete_object(Bucket=RAW_BUCKET, Key=key)

            log.info(f"MOVED TO BRONZE: s3://{BRONZE_BUCKET}/{bronze_key}")

    wait_for_raw_file >> move_files()
