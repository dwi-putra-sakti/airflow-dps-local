from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from plugins.sensors.success_today_sensor import DagSuccessTodaySensor
from datetime import datetime
import sys
sys.path.append("/opt/airflow/src")

from main import mini_consigment_sales 
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
    dag_id='Mini_consi',
    default_args=default_args,
    tags=['db sum > local shared']
) as dag:

    t1 = PythonOperator(
        task_id='mini_consi_task',
        python_callable=mini_consigment_sales,
        op_args=[2025],)
