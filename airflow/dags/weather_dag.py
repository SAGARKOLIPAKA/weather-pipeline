import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner":            "sagar",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}

PROJECT_ROOT = "/opt/airflow"

def fetch_weather():
    result = subprocess.run(
        [sys.executable, f"{PROJECT_ROOT}/dags/scripts/fetch_weather.py"],
        capture_output=True, text=True, cwd=PROJECT_ROOT
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"fetch_weather failed:\n{result.stderr}")

def validate_data():
    result = subprocess.run(
        [sys.executable, f"{PROJECT_ROOT}/dags/scripts/validate_weather.py"],
        capture_output=True, text=True, cwd=PROJECT_ROOT
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"validate_data failed:\n{result.stderr}")

def load_to_postgres():
    result = subprocess.run(
        [sys.executable, f"{PROJECT_ROOT}/dags/scripts/load_to_postgres.py"],
        capture_output=True, text=True, cwd=PROJECT_ROOT
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"load_to_postgres failed:\n{result.stderr}")

def upload_to_azure():
    result = subprocess.run(
        [sys.executable, f"{PROJECT_ROOT}/dags/scripts/upload_to_azure.py"],
        capture_output=True, text=True, cwd=PROJECT_ROOT
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"upload_to_azure failed:\n{result.stderr}")

def dbt_run():
    result = subprocess.run(
        ["dbt", "run", "--project-dir", f"{PROJECT_ROOT}/dags/dbt_transforms"],
        capture_output=True, text=True, cwd=PROJECT_ROOT
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt run failed:\n{result.stderr}")

def dbt_test():
    result = subprocess.run(
        ["dbt", "test", "--project-dir", f"{PROJECT_ROOT}/dags/dbt_transforms"],
        capture_output=True, text=True, cwd=PROJECT_ROOT
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt test failed:\n{result.stderr}")

with DAG(
    dag_id="weather_pipeline",
    description="End-to-end weather data pipeline: ingest, validate, transform",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["weather", "etl", "portfolio"],
) as dag:

    t1_fetch = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )

    t2_validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    t3_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    t4_upload = PythonOperator(
        task_id="upload_to_azure",
        python_callable=upload_to_azure,
    )

    t5_dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=dbt_run,
    )

    t6_dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=dbt_test,
    )

    t1_fetch >> t2_validate >> t3_load >> t4_upload >> t5_dbt_run >> t6_dbt_test
