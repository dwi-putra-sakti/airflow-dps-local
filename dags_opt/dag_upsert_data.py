"""
dags_opt.dag_upsert_data
Optimized Master Data Synchronization DAG.
Runs daily at 06:10 WIB.
Synchronizes Customers, Articles, SO/SA dates, WH Types, Aging, and Expedition Tracking.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

LOCAL_TZ = pendulum.timezone("Asia/Jakarta")

DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(hours=1),
}


# --- Deferred Task Callables ---
def _run_upsert_customers(**context):
    from src_opt.facade import task_upsert_customers
    return task_upsert_customers(**context)


def _run_upsert_artikel(**context):
    from src_opt.facade import task_upsert_artikel
    return task_upsert_artikel(**context)


def _run_upsert_tgl_so_sa(**context):
    from src_opt.facade import task_upsert_tgl_so_sa
    return task_upsert_tgl_so_sa(**context)


def _run_upsert_whtype(**context):
    from src_opt.facade import task_upsert_whtype
    return task_upsert_whtype(**context)


def _run_upsert_master(**context):
    from src_opt.facade import task_upsert_master
    return task_upsert_master(**context)


def _run_aging_gudang(**context):
    from src_opt.facade import task_aging_gudang
    return task_aging_gudang(**context)


def _run_upsert_expedition(**context):
    from src_opt.facade import task_upsert_expedition_tracking
    return task_upsert_expedition_tracking(**context)


with DAG(
    dag_id='opt_upsert_data',
    default_args=DEFAULT_ARGS,
    description='[OPTIMIZED] Sinkronisasi Master Data Harian (06:10 WIB)',
    start_date=datetime(2025, 4, 26, 6, 10, tzinfo=LOCAL_TZ),
    schedule_interval='10 6 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['opt', 'daily', 'master', 'upsert', 'db-sync']
) as dag:

    t_cust = PythonOperator(
        task_id='Upsert_customers_pb',
        python_callable=_run_upsert_customers,
    )

    t_art = PythonOperator(
        task_id='Upsert_artikel_pb',
        python_callable=_run_upsert_artikel,
    )

    t_tgl = PythonOperator(
        task_id='Upsert_tgl_so_sa',
        python_callable=_run_upsert_tgl_so_sa,
    )

    t_whtype = PythonOperator(
        task_id='upsert_whtype',
        python_callable=_run_upsert_whtype,
    )

    t_master = PythonOperator(
        task_id='upsert_master',
        python_callable=_run_upsert_master,
    )

    t_aging = PythonOperator(
        task_id='upsert_aging_gudang_pb',
        python_callable=_run_aging_gudang,
    )

    t_exp = PythonOperator(
        task_id='upsert_expedition_tracking',
        python_callable=_run_upsert_expedition,
    )

    # Execution Sequence
    t_cust >> t_art >> t_tgl >> t_whtype >> t_master >> t_aging >> t_exp
