# my_dag.py (DAG airflow)
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from test_scr import md_artikel_size2  # <--- import fungsi

with DAG(
    dag_id="run_my_script",
    start_date=datetime(2024, 4, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    run_my_script = BashOperator(
        task_id="run_my_script",
        bash_command='python /opt/airflow/dags/test_scr.py'
    )
