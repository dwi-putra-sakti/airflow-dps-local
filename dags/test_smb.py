from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def write_to_smb():
    smb_path = os.getenv("SMB_PATH", "/opt/share")  # fallback default
    file_path = os.path.join(smb_path, "airflow_test.txt")
    with open(file_path, "w") as f:
        f.write("This is a test file written by Airflow DAG.\n")
    print(f"Wrote to {file_path}")

def read_from_smb():
    smb_path = os.getenv("SMB_PATH", "/opt/share")
    file_path = os.path.join(smb_path, "airflow_test.txt")
    with open(file_path, "r") as f:
        content = f.read()
    print(f"Read from {file_path}:\n{content}")

with DAG(
    dag_id="test_smb_access",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "smb"]
) as dag:

    write_task = PythonOperator(
        task_id="write_to_smb",
        python_callable=write_to_smb,
    )

    read_task = PythonOperator(
        task_id="read_from_smb",
        python_callable=read_from_smb,
    )

    write_task >> read_task
