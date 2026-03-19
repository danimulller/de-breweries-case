import sys
import os

sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.ingestion.bronze_pipeline import run_bronze_pipeline

with DAG(
    dag_id="bronze_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "ingestion"],
) as dag:

    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=run_bronze_pipeline
    )