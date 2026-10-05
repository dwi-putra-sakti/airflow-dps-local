"""
dags_opt.dag_local_report_excel
Optimized Local Excel Report Generation DAG.
Runs daily at 07:20 WIB.
Generates store aging, consignment summaries, outstanding OB/DO, and exports to SMB share.
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
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}


# --- Deferred Task Callables ---
def _run_ost_ob(**context):
    from src_opt.facade import task_ost_ob
    return task_ost_ob(**context)


def _run_ost_do(**context):
    from src_opt.facade import task_ost_do
    return task_ost_do(**context)


def _run_stock_borken_aging(**context):
    from src_opt.facade import task_stock_borken_aging
    return task_stock_borken_aging(**context)


def _run_master_gudang(**context):
    from src_opt.facade import task_master_gudang
    return task_master_gudang(**context)


def _run_mini_consi(year=2026, **context):
    from src_opt.facade import task_mini_consigment_sales
    return task_mini_consigment_sales(year=year, **context)


def _run_fstok_gudang(**context):
    from src_opt.facade import task_fstok_gudang
    return task_fstok_gudang(**context)


def _run_md_artsize2(**context):
    from src_opt.facade import task_md_artikel_size2
    return task_md_artikel_size2(**context)


def _run_penjualan_by_table(year=2026, **context):
    from src_opt.facade import task_penjualan_by_table
    return task_penjualan_by_table(year=year, **context)


def _run_md_status_artikel(**context):
    from src_opt.facade import task_md_status_artikel
    return task_md_status_artikel(**context)


def _run_md_artsize26(**context):
    from src_opt.facade import task_md_artikel_size_26
    return task_md_artikel_size_26(**context)


def _run_stock_by_artikel(**context):
    from src_opt.facade import task_stock_by_artikel
    return task_stock_by_artikel(**context)


with DAG(
    dag_id='opt_local_report_excel',
    default_args=DEFAULT_ARGS,
    description='[OPTIMIZED] Laporan Harian Excel ke Shared Storage (07:20 WIB)',
    start_date=datetime(2024, 1, 1, tzinfo=LOCAL_TZ),
    schedule_interval='20 7 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['opt', 'daily', 'report', 'excel', 'smb']
) as dag:

    t_ob = PythonOperator(
        task_id='out_standing_ob_task',
        python_callable=_run_ost_ob,
    )

    t_do = PythonOperator(
        task_id='out_standing_do_task',
        python_callable=_run_ost_do,
    )

    t_broken_aging = PythonOperator(
        task_id='stock_borken_aging_task',
        python_callable=_run_stock_borken_aging,
    )

    t_master_gd = PythonOperator(
        task_id='master_gudang_task',
        python_callable=_run_master_gudang,
    )

    t_mini_consi = PythonOperator(
        task_id='mini_consigment_26_task',
        python_callable=_run_mini_consi,
        op_kwargs={'year': 2026},
    )

    t_fstok_gd = PythonOperator(
        task_id='fstok_gudang_task',
        python_callable=_run_fstok_gudang,
    )

    t_artsize2 = PythonOperator(
        task_id='stock_artsize_md2_task',
        python_callable=_run_md_artsize2,
    )

    t_penjualan_tbl = PythonOperator(
        task_id='penjualan_by_table_task',
        python_callable=_run_penjualan_by_table,
        op_kwargs={'year': 2026},
    )

    t_status_art = PythonOperator(
        task_id='md_status_artikel_task',
        python_callable=_run_md_status_artikel,
    )

    t_artsize26 = PythonOperator(
        task_id='md_artikel_size_26_task',
        python_callable=_run_md_artsize26,
    )

    t_stock_by_art = PythonOperator(
        task_id='stock_by_artikel_task',
        python_callable=_run_stock_by_artikel,
    )

    # Pipeline Chain
    (
        t_ob
        >> t_do
        >> t_broken_aging
        >> t_master_gd
        >> t_mini_consi
        >> t_fstok_gd
        >> t_artsize2
        >> t_penjualan_tbl
        >> t_status_art
        >> t_artsize26
        >> t_stock_by_art
    )
