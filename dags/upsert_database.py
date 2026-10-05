from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
import sys
sys.path.append("/opt/airflow/src")

from main import (sales_to_sql, get_last_three_months_including_current, count_artikel,
                  stock_and_sales, sales_bazar, sales_consinetto,get_stock_monthly_parquet,
                  md_artikel_size,
)
import pendulum

local_tz = pendulum.timezone("Asia/Jakarta")
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 5, 1),
    'retries': 1,
}

with DAG(
    dag_id='upsert_databases',
    default_args=default_args,
    description='DAG yang dijalankan sekali jalan',
    schedule_interval=None,  # bisa diganti cron string juga
    catchup=False,
    tags=['one run']
) as dag:

    # t1_2025 = PythonOperator(
    #     task_id='get_sales_2025',
    #     python_callable=sales_to_sql,
    #     op_args=[2025],
    # )

    # t1_2026 = PythonOperator(
    #     task_id='get_sales_2026',
    #     python_callable=sales_to_sql,
    #     op_args=[2026],
    # )

                        
    # t2 = PythonOperator(task_id='get_stock_task',
    #                     python_callable=get_last_three_months_including_current,)
    # t3_2025 = PythonOperator(
    #     task_id='stock_and_sales_2025',
    #     python_callable=stock_and_sales,
    #     op_args=[2025, 2025],
    # )

    # t3_2026 = PythonOperator(
    #     task_id='stock_and_sales_2026',
    #     python_callable=stock_and_sales,
    #     op_args=[2026, 2026],
    # )

    # t7 = PythonOperator(task_id='get_stock_parquet_task',
    #                     python_callable=get_last_three_months_including_current,
    #                     op_args=['parquet'],)
    t3_2026_parquet = PythonOperator(
        task_id='stock_and_sales_parquet_',
        python_callable=stock_and_sales,
        op_args=[2025, 2025,'parquet'],)
    
    stock_artsize_md25 = PythonOperator(
        task_id='md_artikel_size_25_task',
        python_callable=md_artikel_size,
        op_args=[],)

    # t3_2025_parquet = PythonOperator(task_id='stock_and_sales_2025_parquet',
    #     python_callable=stock_and_sales,op_args=[2025, 2025,'parquet'],)

    # t4 = PythonOperator(task_id='count_artikel_task',
    #                     python_callable=count_artikel, op_args=[],)
    # t5 = PythonOperator(task_id='sales_bazar_task',
    #                     python_callable=sales_bazar, op_args=[], trigger_rule=TriggerRule.ALL_DONE)
    # t6 = PythonOperator(task_id='sales_consinetto_task',
    #                     python_callable=sales_consinetto, op_args=[],trigger_rule=TriggerRule.ALL_DONE)
    
    # t1_2026  >> t5  >> t6 >> t7 >>  t3_2026_parquet >> t4 
    
    # [t1_2025, t1_2026 ] >> t5  >> t6 >> t7 >>  t3_2026_parquet >> t3_2025_parquet >> t4 
    # [t1_2025, t1_2026, t2] >> t3_2026 >> t5 >> t4 >> t6 >> t7 >>  t3_2026_parquet >> t3_2025_parquet

    
    # [t1, t2,] >> t3 >> t5 >> t4 >> t6