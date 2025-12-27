import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from scripts.staging import load_raw_churn_to_duckdb

DBT_PROJECT_DIR = "/opt/airflow/dags/dbt_telecom"
DBT_PROFILES_DIR = DBT_PROJECT_DIR

with DAG(
    dag_id="dbt_telecom_run",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="Run dbt models for telecom ETL",
) as dag:

    load_raw_churn = PythonOperator(
        task_id="load_raw_churn_to_duckdb",
        python_callable=load_raw_churn_to_duckdb,
        op_kwargs={
            "db_path": "data/staging/staging.db",
            "csv_path": "data/raw/customer_churn_data.csv",
        },
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR}",
    )

    load_raw_churn >> dbt_deps >> dbt_run
