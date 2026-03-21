import sys

sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone

from src.ingestion.silver_pipeline import run_silver_pipeline

with DAG(
    dag_id="silver_ingestion",
    start_date=datetime(2026, 3, 20, tzinfo=timezone("America/Sao_Paulo")),
    schedule="@hourly",
    catchup=False,
    default_args={ "retries": 3, "retry_delay": timedelta(minutes=5) },
    tags=["silver", "ingestion"],
) as dag:

    task_1 = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=run_silver_pipeline,
    )
