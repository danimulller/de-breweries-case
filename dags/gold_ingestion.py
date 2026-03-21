import sys
sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone

from src.ingestion.gold_writer import read_silver_parquets, save_to_gold

def task_read_silver_parquets(**context):

    df = read_silver_parquets()

    print(f"Silver rows loaded: {len(df)}")

    # Serialize to JSON so XCom can carry the data between tasks
    context["ti"].xcom_push(key="silver_data", value=df.to_json(orient="records"))


def task_aggregate_and_write(**context):
    import pandas as pd

    silver_json = context["ti"].xcom_pull(
        task_ids="read_silver_parquets",
        key="silver_data"
    )

    df_silver = pd.read_json(silver_json, orient="records")

    object_name = save_to_gold(df_silver)

    print(f"Gold object: {object_name}")


with DAG(
    dag_id="gold_ingestion",
    start_date=datetime(2026, 3, 20, tzinfo=timezone("America/Sao_Paulo")),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["gold", "ingestion"],
) as dag:

    read_silver = PythonOperator(
        task_id="read_silver_parquets",
        python_callable=task_read_silver_parquets,
    )

    write_gold = PythonOperator(
        task_id="aggregate_and_write_to_gold",
        python_callable=task_aggregate_and_write,
    )

    read_silver >> write_gold