import sys

sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone

from src.ingestion.brewery_api import BreweryAPI
from src.ingestion.bronze_writer import save_to_bronze

def task_save_to_bronze(**context):

    api = BreweryAPI()
    response = api.fetch_breweries()

    object_name = save_to_bronze(response)

    print("Bronze ingestion completed successfully!")
    print(f"Records fetched: {response['metadata']['record_count']}")
    print(f"Bronze object path: {object_name}")

with DAG(
    dag_id="bronze_ingestion",
    start_date=datetime(2026, 3, 20, tzinfo=timezone("America/Sao_Paulo")),
    schedule=None,
    catchup=False,
    default_args={ "retries": 3, "retry_delay": timedelta(minutes=5) },
    tags=["bronze", "ingestion"],
) as dag:

    task_1 = PythonOperator(
        task_id="fetch_api_and_save_to_bronze",
        python_callable=task_save_to_bronze
    )