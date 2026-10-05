"""
dags_opt.dag_cleanup_metadata
Optimized Airflow Metadata Maintenance DAG.
Runs daily at midnight to prune high-frequency DAG run logs (e.g. daily_stock runs every 2 mins).
"""
from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session
from airflow.decorators import task, dag
import pendulum

LOCAL_TZ = pendulum.timezone("Asia/Jakarta")


@dag(
    dag_id="opt_clean_old_dag_runs",
    schedule_interval="0 0 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["opt", "maintenance", "cleanup"]
)
def clean_metadata_pipeline():

    @task
    @provide_session
    def cleanup_high_frequency_logs(session=None):
        target_dags = ["daily_stock", "opt_daily_stock"]
        cutoff = days_ago(1)

        for dag_id in target_dags:
            # Delete task instances older than 1 day
            session.query(TaskInstance).filter(
                TaskInstance.execution_date < cutoff,
                TaskInstance.dag_id == dag_id
            ).delete(synchronize_session=False)

            # Delete DAG runs older than 1 day
            session.query(DagRun).filter(
                DagRun.execution_date < cutoff,
                DagRun.dag_id == dag_id
            ).delete(synchronize_session=False)

        session.commit()

    cleanup_high_frequency_logs()


dag_instance = clean_metadata_pipeline()
