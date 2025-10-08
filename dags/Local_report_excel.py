from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.sensors.success_today_sensor import DagSuccessTodaySensor
from datetime import datetime
import sys
sys.path.append("/opt/airflow/src")

from main import md_artikel_size, md_artikel_size2, mini_consigment_sales
import os
from dotenv import load_dotenv
import pendulum

local_tz = pendulum.timezone("Asia/Jakarta")
# Load env variables
load_dotenv(dotenv_path='/opt/airflow/.env')
# Default args
default_args = {
    'owner': 'rivaldi',
    'start_date': datetime(2024, 1, 1, tzinfo=local_tz),
    'retries': 0,
}

# Definisikan DAG
with DAG(
    dag_id='Local_report_excel',
    default_args=default_args,
    schedule_interval='20 7 * * *',  # bisa diganti cron string juga
    catchup=False,
    tags=['db sum > local shared']
) as dag:

    # Buat task untuk menjalankan fungsi Python
    # cek_sales_thru = DagSuccessTodaySensor(
    #     task_id='cek_sales_thru',
    #     external_dag_id='sales_thru_pb ',
    #     poke_interval=300,  # cek setiap 5 menit
    #     timeout=600, # tunggu maksimal 10 menit
    # )
    
    stock_artsize_md1 = PythonOperator(
        task_id='md_artikel_size_task',
        python_callable=md_artikel_size,
        op_args=[],)

    stock_artsize_md2 = PythonOperator(
        task_id='md_artikel_size2_task',
        python_callable=md_artikel_size2,
        op_args=[],)
    
    mini_consigment_sales = PythonOperator(
        task_id='mini_consigment_task',
        python_callable=mini_consigment_sales,
        op_args=[2025],)

    stock_artsize_md1 >> stock_artsize_md2 >> mini_consigment_sales 