from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "hisham",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

GX_PYTHON = "/home/airflow/gx-venv/bin/python" 

with DAG(
    dag_id="ecom_lakehouse_automated_pipeline",
    default_args=default_args,
    description="Bronze -> Silver (Delta MERGE) -> Great Expectations gate -> Gold",
    schedule="@daily",
    start_date=datetime(2026, 10, 3),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "pyspark", "delta", "great-expectations"],
) as dag:

    run_generate_data = BashOperator(
        task_id="generate_data",
        bash_command="python /opt/airflow/ingestion/generate_data.py",
        execution_timeout=timedelta(minutes=10),
    )

   
    run_silver_layer = BashOperator(
        task_id="process_silver_layer",
        bash_command="python /opt/airflow/processing/process_silver.py",
        execution_timeout=timedelta(minutes=30),
    )

    validate_silver = BashOperator(
        task_id="validate_silver_great_expectations",
        bash_command=f"{GX_PYTHON} /opt/airflow/quality/validate_silver_gx.py",
        execution_timeout=timedelta(minutes=15),
    )

    run_gold_layer = BashOperator(
        task_id="process_gold_layer",
        bash_command="python /opt/airflow/processing/process_gold.py",
        execution_timeout=timedelta(minutes=30),
    )

    run_generate_data >> run_silver_layer >> validate_silver >> run_gold_layer
