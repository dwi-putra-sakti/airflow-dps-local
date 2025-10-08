from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append("/opt/airflow/src")

from main import sales_consinetto 
# import os
# from dotenv import load_dotenv
# # Load env variables
# load_dotenv(dotenv_path='/opt/airflow/.env')
# smb_path = os.getenv('SMB_PATH')
# with open('/opt/share/test.txt', 'w') as f:
#     f.write('Hello SMB!')

# Default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Definisikan DAG
with DAG(
    dag_id='sales_consinetto',
    description='DAG untuk menambahkan sales consinetto ke db_sum',
    default_args=default_args,
    schedule_interval=None,  # bisa diganti cron string juga
    catchup=False,
    tags=['one run']
) as dag:

    # Buat task untuk menjalankan fungsi Python
    t1 = PythonOperator(
        task_id='sales_consinetto',
        python_callable=sales_consinetto,
        op_args=[],
    )
    # t1 >> t2 >> 
    t1