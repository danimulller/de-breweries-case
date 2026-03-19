import sys
import os

sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.ingestion.brewery_api import BreweryAPI

def fetch_breweries_task():
    try:
        api = BreweryAPI()

        data = api.fetch_breweries()

        count = data['metadata']['record_count']

        print(f"Retrieved {count} breweries!")
    except Exception as e:
        print(f"Error fetching breweries: {e}")

with DAG(
    dag_id="test_api_connection",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:

    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=fetch_breweries_task
    )