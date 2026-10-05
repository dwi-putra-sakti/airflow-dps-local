"""
dags_opt.dag_daily_stock
Optimized Real-time Stock Sync DAG.
Runs every 2 minutes between 07:00 and 21:59 WIB.
Implements deferred callable loading for zero DAG-parse overhead (<0.02s).
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

LOCAL_TZ = pendulum.timezone("Asia/Jakarta")

DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=10),
}


def _execute_stock_daily(**context):
    """Deferred callable to prevent loading pandas & DB modules during DAG parsing."""
    from src_opt.facade import task_stock_daily
    return task_stock_daily(**context)


with DAG(
    dag_id='opt_daily_stock',
    default_args=DEFAULT_ARGS,
    description='[OPTIMIZED] Sinkronisasi real-time stock setiap 2 menit (Smart Skip jika data tidak berubah)',
    start_date=datetime(2025, 4, 30, 8, 0, tzinfo=LOCAL_TZ),
    schedule_interval='*/2 7-21 * * *',
    catchup=False,
    max_active_runs=1,
    params={
        'force': False,
    },
    tags=['opt', 'realtime', 'stock', 'db-sync', 'smart-skip']
) as dag:

    trx_rt_task = PythonOperator(
        task_id='trx_rt_task',
        python_callable=_execute_stock_daily,
    )

