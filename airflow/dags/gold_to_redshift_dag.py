import time
import boto3
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.sensors.s3 import S3PrefixSensor

# ========================
# CONFIG
# ========================

REGION = "ap-south-1"
S3_GOLD_BUCKET = "ecom-p3-gold"

REDSHIFT_WORKGROUP = "glueproject3-wg"
REDSHIFT_DB = "dev"
REDSHIFT_SECRET_ARN = "arn:aws:secretsmanager:ap-south-1:664751201428:secret:RedshiftServerlessSecret-wcaavv"
IAM_COPY_ROLE = "arn:aws:iam::664751201428:role/RedshiftCopyRole"

TABLES = [
    {"table": "dim_users", "prefix": "dim/dim_users/"},
    {"table": "fact_events", "prefix": "fact/fact_events/"},
    {"table": "fact_transactions", "prefix": "fact/fact_transactions/"},
    {"table": "fact_user_activity", "prefix": "fact/fact_user_activity/"},
    {"table": "daily_revenue", "prefix": "mart/daily_revenue/"},
    {"table": "daily_active_users", "prefix": "mart/daily_active_users/"},
    {"table": "user_ltv", "prefix": "mart/user_ltv/"},
]

rs = boto3.client("redshift-data", region_name=REGION)

# ========================
# HELPERS
# ========================

def run_sql_and_wait(sql: str):
    resp = rs.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DB,
        SecretArn=REDSHIFT_SECRET_ARN,
        Sql=sql,
    )
    stmt_id = resp["Id"]

    while True:
        desc = rs.describe_statement(Id=stmt_id)
        if desc["Status"] == "FINISHED":
            return
        if desc["Status"] in ("FAILED", "ABORTED"):
            raise AirflowFailException(desc.get("Error"))
        time.sleep(2)

# ========================
# SQL
# ========================

CREATE_TABLES_SQL = """
CREATE SCHEMA IF NOT EXISTS ecommerce_gold;

CREATE TABLE IF NOT EXISTS ecommerce_gold.dim_users (
    user_id VARCHAR(50),
    name VARCHAR(200),
    email VARCHAR(200),
    city VARCHAR(150),
    phone VARCHAR(50),
    age VARCHAR(50),
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ecommerce_gold.fact_events (
    event_id VARCHAR(100),
    user_id VARCHAR(50),
    ts TIMESTAMP,
    event_date DATE,
    country VARCHAR(100),
    user_city VARCHAR(200),
    user_age VARCHAR(50),
    user_email VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS ecommerce_gold.fact_transactions (
    tx_id VARCHAR(100),
    user_id VARCHAR(50),
    amount DOUBLE PRECISION,
    status VARCHAR(50),
    created_at TIMESTAMP,
    delivered_at TIMESTAMP,
    tx_date DATE,
    user_city VARCHAR(200),
    user_age VARCHAR(50),
    user_email VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS ecommerce_gold.fact_user_activity (
    user_id VARCHAR(50),
    date DATE,
    events_count BIGINT,
    transactions_count BIGINT,
    total_revenue DOUBLE PRECISION,
    avg_transaction_amount DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS ecommerce_gold.daily_revenue (
    date DATE,
    transaction_count BIGINT,
    total_revenue DOUBLE PRECISION,
    avg_amount DOUBLE PRECISION,
    delivered_count BIGINT
);

CREATE TABLE IF NOT EXISTS ecommerce_gold.daily_active_users (
    date DATE,
    event_active_users BIGINT,
    tx_active_users BIGINT,
    total_active_users BIGINT
);

CREATE TABLE IF NOT EXISTS ecommerce_gold.user_ltv (
    user_id VARCHAR(50),
    event_count BIGINT,
    first_event TIMESTAMP,
    last_event TIMESTAMP,
    transaction_count BIGINT,
    total_revenue DOUBLE PRECISION,
    ltv DOUBLE PRECISION
);
"""

# ========================
# DAG
# ========================

default_args = {
    "owner": "pranit",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="gold_to_redshift_serverless_loader_with_validation",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    @task
    def create_tables():
        run_sql_and_wait(CREATE_TABLES_SQL)

    wait_for_s3 = S3PrefixSensor.partial(
        aws_conn_id="aws_default",
        bucket_name=S3_GOLD_BUCKET,
        poke_interval=30,
        timeout=60 * 60,
        mode="reschedule",
    ).expand(
        task_id=[f"wait_{t['table']}_s3" for t in TABLES],
        prefix=[t["prefix"] for t in TABLES],
    )

    @task
    def load_table(table_cfg: dict):
        table = table_cfg["table"]
        prefix = table_cfg["prefix"]

        run_sql_and_wait(f"TRUNCATE TABLE ecommerce_gold.{table};")

        run_sql_and_wait(f"""
            COPY ecommerce_gold.{table}
            FROM 's3://{S3_GOLD_BUCKET}/{prefix}'
            IAM_ROLE '{IAM_COPY_ROLE}'
            FORMAT PARQUET
            REGION '{REGION}';
        """)

        return table

    @task
    def validate_tables(tables: list):
        for table in tables:
            resp = rs.execute_statement(
                WorkgroupName=REDSHIFT_WORKGROUP,
                Database=REDSHIFT_DB,
                SecretArn=REDSHIFT_SECRET_ARN,
                Sql=f"SELECT COUNT(*) FROM ecommerce_gold.{table};",
            )
            stmt_id = resp["Id"]

            while True:
                desc = rs.describe_statement(Id=stmt_id)
                if desc["Status"] == "FINISHED":
                    break
                if desc["Status"] in ("FAILED", "ABORTED"):
                    raise AirflowFailException(f"Validation failed for {table}")
                time.sleep(2)

            rows = rs.get_statement_result(Id=stmt_id)["Records"]
            count = int(rows[0][0]["longValue"])

            if count == 0:
                raise AirflowFailException(f"{table} has ZERO rows")

    # ========================
    # FLOW
    # ========================

    create_tables() >> wait_for_s3

    loaded = load_table.expand(
        task_id=[f"load_{t['table']}" for t in TABLES],
        table_cfg=TABLES,
    )

    wait_for_s3 >> loaded >> validate_tables(loaded)
