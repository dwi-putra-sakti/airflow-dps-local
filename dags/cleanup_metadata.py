from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session
from airflow.decorators import task, dag

# DAG pembersihan
@dag(
    schedule_interval="0 0 * * *",  # Setiap hari pada tengah malam
    start_date=days_ago(1),
    catchup=False,  # Jangan lakukan run backfill
    tags=["cleanup"]
)
def clean_old_dag_runs():
    
    @task
    @provide_session
    def cleanup(session=None):
        # Hapus task instances yang lebih dari 3 hari
        session.query(TaskInstance).filter(
            TaskInstance.execution_date < days_ago(1),
            TaskInstance.dag_id == "daily_stock"  # ganti dengan dag_id kamu
        ).delete(synchronize_session=False)

        # Hapus DAG run yang lebih dari 3 hari
        session.query(DagRun).filter(
            DagRun.execution_date < days_ago(1),
            DagRun.dag_id == "daily_stock"  # ganti dengan dag_id kamu
        ).delete(synchronize_session=False)

        session.commit()

    cleanup()

# Membuat instance DAG pembersihan
dag_instance = clean_old_dag_runs()
