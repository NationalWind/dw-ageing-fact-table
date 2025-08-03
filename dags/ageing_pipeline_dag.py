from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 1),
}

with DAG('ageing_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    run_dbt = BashOperator(
        task_id='run_dbt_transform',
        bash_command='dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt'
    )

    run_dbt
