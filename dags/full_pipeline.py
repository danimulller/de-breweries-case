import sys
sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone

from src.ingestion.brewery_api import BreweryAPI
from src.ingestion.bronze_writer import save_to_bronze
from src.ingestion.silver_writer import save_to_silver
from src.ingestion.gold_writer import read_silver_parquets, save_to_gold

def task_fetch_and_save_to_bronze(**context):
    
    api = BreweryAPI()
    response = api.fetch_breweries()

    object_name = save_to_bronze(response)

    print(f"Records fetched: {response['metadata']['record_count']}")
    print(f"Bronze object path: {object_name}")

    context["ti"].xcom_push(key="bronze_object_name", value=object_name)

def task_transform_bronze_to_silver(**context):
    
    object_name = context["ti"].xcom_pull(
        task_ids="fetch_and_save_to_bronze",
        key="bronze_object_name"
    )

    output_path = save_to_silver(object_name)

    print(f"Silver output path: {output_path}")

def task_aggregate_and_write_to_gold(**context):
    
    df_silver = read_silver_parquets()
    object_name = save_to_gold(df_silver)

    print(f"Gold object: {object_name}")

with DAG(
    dag_id="full_pipeline",
    start_date=datetime(2026, 3, 20, tzinfo=timezone("America/Sao_Paulo")),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "silver", "gold", "pipeline"],
) as dag:

    bronze = PythonOperator(
        task_id="fetch_and_save_to_bronze",
        python_callable=task_fetch_and_save_to_bronze,
    )

    silver = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=task_transform_bronze_to_silver,
    )

    gold = PythonOperator(
        task_id="aggregate_and_write_to_gold",
        python_callable=task_aggregate_and_write_to_gold,
    )

    bronze >> silver >> gold