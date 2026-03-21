import sys
sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone

from src.ingestion.silver_writer import get_latest_bronze_object, save_to_silver

def task_get_latest_bronze_object(**context):
    
    object_name = get_latest_bronze_object()

    print(f"Latest bronze object: {object_name}")

    # Push to XCom so the next task can pick it up
    context["ti"].xcom_push(key="bronze_object_name", value=object_name)

def task_save_to_silver(**context):

    object_name = context["ti"].xcom_pull(
        task_ids="get_latest_bronze_object",
        key="bronze_object_name"
    )

    output_path = save_to_silver(object_name)
    
    print(f"Silver output path: {output_path}")

with DAG(
    dag_id="silver_ingestion",
    start_date=datetime(2026, 3, 20, tzinfo=timezone("America/Sao_Paulo")),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["silver", "ingestion"],
) as dag:

    get_bronze = PythonOperator(
        task_id="get_latest_bronze_object",
        python_callable=task_get_latest_bronze_object,
    )

    write_silver = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=task_save_to_silver,
    )

    get_bronze >> write_silver