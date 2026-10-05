from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
import sys
sys.path.append("/opt/airflow/src")

from main import (penjualan_by_table,
)
import pendulum

local_tz = pendulum.timezone("Asia/Jakarta")
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 7, 1),
    'retries': 1,
}

with DAG(
    dag_id='one_run_penjualan_table',
    default_args=default_args,
    description='DAG yang dijalankan sekali jalan',
    schedule_interval=None,  # bisa diganti cron string juga
    catchup=False,
    tags=['one run']
) as dag:
    t1_2026 = PythonOperator(
            task_id='penjualan_yearly',
            python_callable=penjualan_by_table,
            op_args=[2026],
        )