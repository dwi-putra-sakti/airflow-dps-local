from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
import sys
sys.path.append("/opt/airflow/src")

from main import (sales_to_sql, get_last_three_months_including_current, stock_and_sales, sales_bazar, sales_consinetto,
count_artikel)
import pendulum

local_tz = pendulum.timezone("Asia/Jakarta")
default_args = {
    'owner': 'rivaldi',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sales_thru_pb',
    default_args=default_args,
    description='DAG yang dijalankan setiap hari jam 6:20 pagi',
    start_date=datetime(2025, 4, 26, 6, 20, tzinfo=local_tz),  # waktu lokal
    schedule_interval='20 6 * * *',
    catchup=False,
    tags=['db syncro > db sum']
) as dag:

    t1 = PythonOperator(task_id='gat_sales_task', 
                        python_callable=sales_to_sql, op_args=[2025],)
    t2 = PythonOperator(task_id='get_stock_task',
                        python_callable=get_last_three_months_including_current,)
    t3 = PythonOperator(task_id='stock_and_sales_task',
                        python_callable=stock_and_sales, op_args=[2025,2025],)
    t4 = PythonOperator(task_id='count_artikel_task',
                        python_callable=count_artikel, op_args=[],)
    t5 = PythonOperator(task_id='sales_bazar_task',
                        python_callable=sales_bazar, op_args=[], trigger_rule=TriggerRule.ALL_DONE)
    t6 = PythonOperator(task_id='sales_consinetto_task',
                        python_callable=sales_consinetto, op_args=[],trigger_rule=TriggerRule.ALL_DONE)
    [t1, t2] >> t3 >> t5 >> t4 >> t6
    