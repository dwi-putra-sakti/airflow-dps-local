from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append("/opt/airflow/src")

from main import disc2nd_consi_online 
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
    dag_id='disc2nd_consi_online',
    description='DAG untuk menambakan disc2nd consi dan online ke db_sum',
    default_args=default_args,
    schedule_interval=None,  # bisa diganti cron string juga
    catchup=False,
    tags=['one run']
) as dag:

    # Buat task untuk menjalankan fungsi Python
    t1 = PythonOperator(
        task_id='disc2nd_consi_online',
        python_callable=disc2nd_consi_online,
        op_args=[],
    )
    # t1 >> t2 >> 
    t1