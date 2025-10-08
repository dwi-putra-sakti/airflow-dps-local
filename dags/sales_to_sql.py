from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append("/opt/airflow/src")

from main import sales_to_sql 
import os
from dotenv import load_dotenv
# Load env variables
load_dotenv(dotenv_path='/opt/airflow/.env')
smb_path = os.getenv('SMB_PATH')
with open('/opt/share/test.txt', 'w') as f:
    f.write('Hello SMB!')

# Default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Definisikan DAG
with DAG(
    dag_id='sales_to_sql',
    description='DAG untuk mengeksekusi fungsi sales_to_sql',
    default_args=default_args,
    schedule_interval=None,  # bisa diganti cron string juga
    catchup=False,
    tags=['one run']
) as dag:

    # Buat task untuk menjalankan fungsi Python
    t1 = PythonOperator(
        task_id='sales_to_sql_task_2025',
        python_callable=sales_to_sql,
        op_args=[2025],
    )
    t2 = PythonOperator(
        task_id='sales_to_sql_task_2024',
        python_callable=sales_to_sql,
        op_args=[2024],
    )
    t3 = PythonOperator(
        task_id='sales_to_sql_task_2023',
        python_callable=sales_to_sql,
        op_args=[2023],
    )   
    # t1 >> t2 >> 
    t3