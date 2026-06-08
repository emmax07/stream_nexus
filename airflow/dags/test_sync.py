from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="01_desktop_to_docker_test",
    start_date=datetime(2026, 1, 1),
    schedule=None, # Only runs when you click play manually
    catchup=False,
) as dag:

    start_task = EmptyOperator(task_id="start_pipeline")
    end_task = EmptyOperator(task_id="end_pipeline")

    start_task >> end_task