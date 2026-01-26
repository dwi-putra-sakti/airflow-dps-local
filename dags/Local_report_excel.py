from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.sensors.success_today_sensor import DagSuccessTodaySensor
from datetime import datetime
import sys
sys.path.append("/opt/airflow/src")

from main import (md_artikel_size, md_artikel_size2, mini_consigment_sales, ost_ob,ost_do,
                  stock_borken_aging,master_gudang,md_artikel_size_26)
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
    
    stock_artsize_md26 = PythonOperator(
        task_id='md_artikel_size_26_task',
        python_callable=md_artikel_size_26,
        op_args=[],)

    stock_artsize_md2 = PythonOperator(
        task_id='md_artikel_size2_task',
        python_callable=md_artikel_size2,
        op_args=[],)
    
    mini_consigment_task_25 = PythonOperator(
        task_id='mini_consigment_task',
        python_callable=mini_consigment_sales,
        op_args=[2025],
        )

    mini_consigment_task_26 = PythonOperator(
        task_id='mini_consigment_26_task',
        python_callable=mini_consigment_sales,
        op_args=[2026],
    )

    out_standing_ob = PythonOperator(
        task_id='out_standing_ob_task',
        python_callable=ost_ob,
        op_args=[],)

    out_standing_do = PythonOperator(
        task_id='out_standing_do_task',
        python_callable=ost_do,
        op_args=[],)
    
    stock_borken_aging = PythonOperator(
        task_id='stock_borken_aging_task',
        python_callable=stock_borken_aging,
        op_args=[],)

    master_gudang = PythonOperator(
        task_id='master_gudang_task',
        python_callable=master_gudang,
        op_args=[],)
    
    (stock_artsize_md1 >> stock_artsize_md2 >> mini_consigment_task_25 >> out_standing_ob >> 
     out_standing_do >> stock_borken_aging >> master_gudang >> stock_artsize_md26 >> mini_consigment_task_26)