
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import check_kafka as ck
import check_snowflake as cs

default_args = {
    "owner": "Emmax",
    "depends_on_past": False,
    "retries": 3,                             # Automatically try again up to 3 times
    "retry_delay": timedelta(minutes=2),      # Wait 2 minutes between retry attempts
    "execution_timeout": timedelta(minutes=5) # Prevent tasks from hanging indefinitely
}

with DAG(
    dag_id="ride_pipeline_monitor",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly", # Runs automatically every hour
    catchup=False,
    tags=["monitoring", "stream_nexus"]
) as dag:

    # Task 1: Audit Kafka Infrastructure Health
    kafka_check_task = PythonOperator(
        task_id="verify_kafka_broker",
        python_callable=ck.verify_kafka
    )

    # Task 2: Validate Data Accumulation and Performance inside Snowflake
    snowflake_check_task = PythonOperator(
        task_id="verify_snowflake_data_load",
        python_callable=cs.verify_load
    )

    # Workflow Dependency Chain
    # Airflow verifies Kafka is alive first; if true, it immediately checks Snowflake ingest health
    kafka_check_task >> snowflake_check_task