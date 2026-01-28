#users_ingestion_dag

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

RAW_PREFIX    = "users/"
BRONZE_PREFIX = "users/"

s3 = boto3.client("s3", region_name="ap-south-1")
log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)


# ===================== HELPERS ===================== #

def list_raw_files():
    keys = []
    token = None
    while True:
        resp = (
            s3.list_objects_v2(
                Bucket=RAW_BUCKET,
                Prefix=RAW_PREFIX,
                ContinuationToken=token,
            )
            if token
            else s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix=RAW_PREFIX)
        )

        keys.extend([
            o["Key"] for o in resp.get("Contents", [])
            if not o["Key"].endswith("/")
        ])

        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

    return keys


def valid_format(key: str) -> bool:
    key = key.lower()

    # skip parquet dataset metadata
    if any(x in key for x in ["_success", ".crc"]):
        return False

    # real parquet file
    if key.endswith(".parquet") and "part-" in key:
        return True

    # allowed raw formats
    return key.endswith((
        ".csv", ".csv.gz",
        ".json",
        ".ndjson", ".ndjson.gz",
        ".txt",
        ".gz",
    ))


def try_read(bucket, key):
    s3.get_object(Bucket=bucket, Key=key)  # just readability check
    return True


# ===================== EVENT-DRIVEN DAG ===================== #

default_args = {"owner": "pranit", "retries": 1}

with DAG(
    dag_id="raw_to_bronze_users_event_driven",
    description="Event-driven RAW → BRONZE for users",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,               # event-driven
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
):

    # EVENT TRIGGER: runs when ANY file arrives at RAW/users/*
    wait_for_raw_file = S3KeySensor(
        task_id="wait_for_raw_user_file",
        bucket_key=f"{RAW_PREFIX}*",
        bucket_name=RAW_BUCKET,
        wildcard_match=True,
        poke_interval=10,
        timeout=60 * 60 * 24,
        soft_fail=False,
    )

    @task()
    def move_files():
        files = list_raw_files()
        if not files:
            log.info("No RAW user files.")
            return

        for key in files:

            # invalid format → quarantine
            if not valid_format(key):
                s3.copy_object(
                    Bucket=QUARANTINE_BUCKET,
                    CopySource={"Bucket": RAW_BUCKET, "Key": key},
                    Key=key,
                )
                s3.delete_object(Bucket=RAW_BUCKET, Key=key)
                log.info(f"INVALID FORMAT → QUARANTINE: {key}")
                continue

            # unreadable → quarantine
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

            # valid file → bronze
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
