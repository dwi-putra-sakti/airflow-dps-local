from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from plugins.sensors.success_today_sensor import DagSuccessTodaySensor
from datetime import datetime
import sys
sys.path.append("/opt/airflow/src")

from main import md_artikel_size, md_artikel_size2 
from modules.proses import test_get_stock_data
import os
from dotenv import load_dotenv
import pendulum
from airflow.utils.state import DagRunState

local_tz = pendulum.timezone("Asia/Jakarta")
# Load env variables
load_dotenv(dotenv_path='/opt/airflow/.env')
def hari_ini_midnight(execution_date, **kwargs):
    # Ambil jam 00:00 hari ini
    return execution_date.replace(hour=0, minute=0, second=0, microsecond=0)
# Default args
default_args = {
    'owner': 'rivaldi',
    'start_date': datetime(2024, 1, 1, tzinfo=local_tz),
    'retries': 0,
}

# Definisikan DAG
with DAG(
    dag_id='Stock_artsize_md',
    default_args=default_args,
    tags=['db sum > local shared']
) as dag:

    # Buat task untuk menjalankan fungsi Python
    cek_sales_thru = DagSuccessTodaySensor(
        task_id='cek_sales_thru',
        external_dag_id='sales_thru_pb',
        poke_interval=300,  # cek setiap 5 menit
        timeout=600, # tunggu maksimal 10 menit
    )
    
    t1 = PythonOperator(
        task_id='md_artikel_size_task',
        python_callable=md_artikel_size,
        op_args=[],)

    t2 = PythonOperator(
        task_id='md_artikel_size2_task',
        python_callable=md_artikel_size2,
        op_args=[],)

    cek_sales_thru >> t1 >> t2