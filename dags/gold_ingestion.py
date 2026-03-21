import sys

sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone

from src.ingestion.gold_pipeline import run_gold_pipeline

with DAG(
    dag_id="gold_ingestion",
    start_date=datetime(2026, 3, 20, tzinfo=timezone("America/Sao_Paulo")),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["gold", "ingestion"],
) as dag:

    task_1 = PythonOperator(
        task_id="aggregate_silver_to_gold",
        python_callable=run_gold_pipeline,
    )
