from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from plugins.sensors.success_today_sensor import DagSuccessTodaySensor
from datetime import datetime, timedelta
import sys
sys.path.append("/opt/airflow/src")

from main import (upsert_artikel_pb , upsert_customers_pb, upsert_tgl_so_sa, upsert_whtype, 
                upsert_master,aging_gudang_pb,upsert_expedition_tracking)
import os
from dotenv import load_dotenv
import pendulum
from airflow.utils.state import DagRunState

local_tz = pendulum.timezone("Asia/Jakarta")
# Load env variables
load_dotenv(dotenv_path='/opt/airflow/.env')

default_args = {
    'owner': 'rivaldi',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definisikan DAG
with DAG(
    dag_id='Upsert_data',
    default_args=default_args,
    start_date=datetime(2025, 4, 26, 6, 20, tzinfo=local_tz),  # waktu lokal
    schedule_interval='10 6 * * *',
    catchup=False,
    tags=['db syncro > db sum']
) as dag:

    t1 = PythonOperator(
        task_id='Upsert_artikel_pb',
        python_callable=upsert_artikel_pb,
        op_args=[],)
    
    t2 = PythonOperator(
        task_id='Upsert_customers_pb',
        python_callable=upsert_customers_pb,
        op_args=[],)
    
    t3 = PythonOperator(
        task_id='Upsert_tgl_so_sa',
        python_callable=upsert_tgl_so_sa,
        op_args=[],)
    
    t4 = PythonOperator(
        task_id='upsert_whtype',
        python_callable=upsert_whtype,
        op_args=[],)
    
    t5 = PythonOperator(
        task_id='upsert_master',
        python_callable=upsert_master,
        op_args=[],)  
    
    t6 = PythonOperator(
        task_id='upsert_aging_gudang_pb',
        python_callable=aging_gudang_pb,
        op_args=[],)  

    t7 = PythonOperator(
        task_id='upsert_expedition_tracking',
        python_callable=upsert_expedition_tracking,
        op_args=[],) 
    t2>>t1>>t3>>t4>>t5>>t6>>t7