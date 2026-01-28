import time
import boto3
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException

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
    {"name": "dim_users", "prefix": "dim/dim_users/"},
    {"name": "fact_events", "prefix": "fact/fact_events/"},
    {"name": "fact_transactions", "prefix": "fact/fact_transactions/"},
    {"name": "fact_user_activity", "prefix": "fact/fact_user_activity/"},
    {"name": "daily_revenue", "prefix": "mart/daily_revenue/"},
    {"name": "daily_active_users", "prefix": "mart/daily_active_users/"},
    {"name": "user_ltv", "prefix": "mart/user_ltv/"},
]

rs = boto3.client("redshift-data", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

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
    statement_id = resp["Id"]

    while True:
        desc = rs.describe_statement(Id=statement_id)
        status = desc["Status"]

        if status == "FINISHED":
            return True
        if status in ["FAILED", "ABORTED"]:
            raise AirflowFailException(f"SQL failed: {desc.get('Error')}")
        time.sleep(2)

def check_s3(path: str) -> bool:
    resp = s3.list_objects_v2(Bucket=S3_GOLD_BUCKET, Prefix=path)
    return any(obj["Size"] > 0 for obj in resp.get("Contents", []))

# ========================
# DAG
# ========================
default_args = {
    "owner": "pranit",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="gold_to_redshift_dynamic_loader",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    @task()
    def create_tables():
        CREATE_TABLES_SQL = """
        CREATE SCHEMA IF NOT EXISTS ecommerce_gold;
        CREATE TABLE IF NOT EXISTS ecommerce_gold.dim_users (
            user_id VARCHAR(50), name VARCHAR(200), email VARCHAR(200),
            city VARCHAR(150), phone VARCHAR(50), age VARCHAR(50),
            updated_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS ecommerce_gold.fact_events (
            event_id VARCHAR(100), user_id VARCHAR(50), ts TIMESTAMP,
            event_date DATE, country VARCHAR(100), user_city VARCHAR(200),
            user_age VARCHAR(50), user_email VARCHAR(200)
        );
        CREATE TABLE IF NOT EXISTS ecommerce_gold.daily_revenue (
            date DATE, transaction_count BIGINT, total_revenue DOUBLE PRECISION,
            avg_amount DOUBLE PRECISION, delivered_count BIGINT
        );
        CREATE TABLE IF NOT EXISTS ecommerce_gold.fact_transactions (
            tx_id VARCHAR(100), user_id VARCHAR(50), amount DOUBLE PRECISION,
            status VARCHAR(50), created_at TIMESTAMP, delivered_at TIMESTAMP,
            tx_date DATE, user_city VARCHAR(200), user_age VARCHAR(50),
            user_email VARCHAR(200)
        );
        CREATE TABLE IF NOT EXISTS ecommerce_gold.fact_user_activity (
            user_id VARCHAR(50), date DATE, events_count BIGINT,
            transactions_count BIGINT, total_revenue DOUBLE PRECISION,
            avg_transaction_amount DOUBLE PRECISION
        );
        CREATE TABLE IF NOT EXISTS ecommerce_gold.daily_active_users (
            date DATE, event_active_users BIGINT, tx_active_users BIGINT,
            total_active_users BIGINT
        );
        CREATE TABLE IF NOT EXISTS ecommerce_gold.user_ltv (
            user_id VARCHAR(50), event_count BIGINT, first_event TIMESTAMP,
            last_event TIMESTAMP, transaction_count BIGINT, total_revenue DOUBLE PRECISION,
            ltv DOUBLE PRECISION
        );
        """
        run_sql_and_wait(CREATE_TABLES_SQL)

    @task()
    def wait_for_s3_table(table: dict):
        while not check_s3(table["prefix"]):
            log.info(f"Waiting for S3 files: {table['prefix']}")
            time.sleep(15)
        return table["name"], table["prefix"]

    @task()
    def load_table(table_info: tuple):
        table_name, prefix = table_info
        run_sql_and_wait(f"TRUNCATE TABLE ecommerce_gold.{table_name};")
        run_sql_and_wait(f"""
            COPY ecommerce_gold.{table_name}
            FROM 's3://{S3_GOLD_BUCKET}/{prefix}'
            IAM_ROLE '{IAM_COPY_ROLE}'
            FORMAT PARQUET;
        """)
        return table_name

    @task()
    def validate(tables: list):
        counts = {}
        for table in tables:
            resp = rs.execute_statement(
                WorkgroupName=REDSHIFT_WORKGROUP,
                Database=REDSHIFT_DB,
                SecretArn=REDSHIFT_SECRET_ARN,
                Sql=f"SELECT COUNT(*) FROM ecommerce_gold.{table};"
            )
            stmt_id = resp["Id"]
            while True:
                desc = rs.describe_statement(Id=stmt_id)
                if desc["Status"] == "FINISHED":
                    break
                if desc["Status"] in ["FAILED", "ABORTED"]:
                    raise AirflowFailException(f"Validation failed for {table}")
                time.sleep(2)
            rows = rs.get_statement_result(Id=stmt_id)["Records"]
            counts[table] = int(rows[0][0]["longValue"])
            log.info(f"Table {table} row count: {counts[table]}")
            if counts[table] == 0:
                raise AirflowFailException(f"{table} has zero rows!")
        return counts

    # DAG flow
    tables_ready = wait_for_s3_table.expand(table=TABLES)
    loaded_tables = load_table.expand(table_info=tables_ready)
    create_tables() >> tables_ready >> loaded_tables >> validate(loaded_tables)
