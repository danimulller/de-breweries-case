from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Airflow is working!")

with DAG(
    dag_id="test_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:

    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=hello
    )