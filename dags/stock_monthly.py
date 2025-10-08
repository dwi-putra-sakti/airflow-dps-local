from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append("/opt/airflow/src")

from main import get_last_three_months_including_current

# Default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Definisikan DAG
with DAG(
    dag_id='2run_main_py_dag',
    default_args=default_args,
    schedule_interval='@daily',  # bisa diganti cron string juga
    catchup=False,
    tags=['example']
) as dag:

    test_run_job_task = PythonOperator(
        task_id='get_last_three_months_including_current',
        python_callable=get_last_three_months_including_current,
    )

    test_run_job_task