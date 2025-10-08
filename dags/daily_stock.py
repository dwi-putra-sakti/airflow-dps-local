from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append("/opt/airflow/src")
import pandas as pd
from main import stock_daily, sales_daily
from modules.queries import df__rtt
import pendulum

local_tz = pendulum.timezone("Asia/Jakarta")
default_args = {
    'owner': 'rivaldi',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

    
with DAG(
    dag_id='daily_stock',
    default_args=default_args,
    description='DAG yang dijalankan setiap 2 menit',
    start_date=datetime(2025, 4, 30, 8, 10, tzinfo=local_tz),  # waktu lokal
    schedule_interval='*/2 7-21 * * *',
    catchup=False,
    tags=['db real-time > db sum']
) as dag:

    t1 = PythonOperator(task_id='trx_rt_task', 
                        python_callable=stock_daily, op_args=[],)
    t2 = PythonOperator(task_id='sls_rt_task', 
                        python_callable=sales_daily, op_args=[2025],)
    t1>>t2