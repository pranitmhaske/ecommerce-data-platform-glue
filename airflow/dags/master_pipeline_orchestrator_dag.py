from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    "owner": "pranit",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="master_pipeline_orchestrator_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,     # manual or API-triggered
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    description="Master orchestrator for Bronze→Silver→Gold→Redshift pipeline",
) as dag:

    # TRIGGER BRONZE → SILVER (Glue Spark Job)
    tr_bronze_silver = TriggerDagRunOperator(
        task_id="trigger_bronze_to_silver",
        trigger_dag_id="glue_bronze_to_silver_spark",
        wait_for_completion=True,
    )

    # TRIGGER SILVER → GOLD (Glue Spark Job)
    tr_silver_gold = TriggerDagRunOperator(
        task_id="trigger_silver_to_gold",
        trigger_dag_id="glue_silver_to_gold_spark",
        wait_for_completion=True,
    )

    # TRIGGER GOLD → REDSHIFT LOAD
    tr_gold_redshift = TriggerDagRunOperator(
        task_id="trigger_gold_to_redshift",
        trigger_dag_id="glue_gold_to_redshift_serverless_loader",
        wait_for_completion=True,
    )
    tr_bronze_silver >> tr_silver_gold >> tr_gold_redshift
