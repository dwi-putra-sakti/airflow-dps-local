from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append("/opt/airflow/src")

from main import stock_and_sales 
from modules.proses import test_get_stock_data
import os
from dotenv import load_dotenv
# Load env variables
load_dotenv(dotenv_path='/opt/airflow/.env')

# Default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

# Definisikan DAG
with DAG(
    dag_id='3run_main_py_dag',
    default_args=default_args,
    schedule_interval='@daily',  # bisa diganti cron string juga
    catchup=False,
    tags=['example']
) as dag:

    # Buat task untuk menjalankan fungsi Python
    run_job_task = PythonOperator(
        task_id='stock_and_sales_task',
        python_callable=stock_and_sales,
        op_args=[2025,2025],)

    # test_job_task = PythonOperator(
    #     task_id='test_get_stock_data_task',
    #     python_callable=test_get_stock_data,
    #     op_args=[],    
    # )

    run_job_task