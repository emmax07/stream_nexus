from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import socket
from datetime import timedelta
import check_kafka as ck
import check_snowflake as cs


# Airflow Default Settings
default_args = {
    "owner": "Emmax",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2)
}


# Functions

# ck.verify_kafka()
# cs.verify_load()
   

def start_producer():
    subprocess.run(
        ["python", "..src/producer.py"],
        check=True
    )


def start_consumer():
    subprocess.run(
        ["python", "..src/consumer.py"],
        check=True
    )

# DAG

with DAG(
    dag_id="ride_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False
) as dag:

        # Task 1
    kafka_check = PythonOperator(
        task_id="verify_kafka",
        python_callable=ck.verify_kafka
    )

    # Task 2
    snowflake_check = PythonOperator(
    task_id="verify_snowflake_load",
    python_callable=cs.verify_load
    )
    
    # Task 3
    producer_task = PythonOperator(
        task_id="start_producer",
        python_callable=start_producer
    )

    # Task 4
    consumer_task = PythonOperator(
        task_id="start_consumer",
        python_callable=start_consumer
    )

    # Workflow Order
    kafka_check >> producer_task >> consumer_task
