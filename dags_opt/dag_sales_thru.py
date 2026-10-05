"""
dags_opt.dag_sales_thru
Optimized Sales Thru Pipeline DAG.
Runs daily at 06:20 WIB.
Executes sales to SQL, secondary discounts, stock & sales parquet aggregation, and count articles.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
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
def _run_sales_to_sql(year=2026, **context):
    from src_opt.facade import task_sales_to_sql
    return task_sales_to_sql(year=year, **context)


def _run_sales_bazar(**context):
    from src_opt.facade import task_sales_bazar
    return task_sales_bazar(**context)


def _run_sales_consinetto(**context):
    from src_opt.facade import task_sales_consinetto
    return task_sales_consinetto(**context)


def _run_stock_parquet(**context):
    from src_opt.facade import task_stock_monthly
    return task_stock_monthly(typefile='parquet', **context)


def _run_stock_and_sales_parquet(start_year=2026, end_year=2026, **context):
    from src_opt.facade import task_stock_and_sales
    return task_stock_and_sales(start_year=start_year, end_year=end_year, typefile='parquet', **context)


def _run_count_artikel(**context):
    from src_opt.facade import task_count_artikel
    return task_count_artikel(**context)


def _run_sales_ptsbz(**context):
    from src_opt.facade import task_sales_ptsbz
    return task_sales_ptsbz(**context)


def _run_sales_mp(**context):
    from src_opt.facade import task_sales_mp
    return task_sales_mp(**context)


with DAG(
    dag_id='opt_sales_thru_pb',
    default_args=DEFAULT_ARGS,
    description='[OPTIMIZED] Pipeline Sales Thru Harian (06:20 WIB)',
    start_date=datetime(2025, 4, 26, 6, 20, tzinfo=LOCAL_TZ),
    schedule_interval='20 6 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['opt', 'daily', 'sales', 'parquet', 'db-sync']
) as dag:

    t_sales_2026 = PythonOperator(
        task_id='get_sales_2026',
        python_callable=_run_sales_to_sql,
        op_kwargs={'year': 2026},
    )

    t_sales_bazar = PythonOperator(
        task_id='sales_bazar_task',
        python_callable=_run_sales_bazar,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_sales_consinetto = PythonOperator(
        task_id='sales_consinetto_task',
        python_callable=_run_sales_consinetto,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_stock_parquet = PythonOperator(
        task_id='get_stock_parquet_task',
        python_callable=_run_stock_parquet,
    )

    t_stock_and_sales = PythonOperator(
        task_id='stock_and_sales_2026_parquet',
        python_callable=_run_stock_and_sales_parquet,
        op_kwargs={'start_year': 2026, 'end_year': 2026},
    )

    t_count_artikel = PythonOperator(
        task_id='count_artikel_task',
        python_callable=_run_count_artikel,
    )

    t_sales_ptsbz = PythonOperator(
        task_id='sales_ptsbz_task',
        python_callable=_run_sales_ptsbz,
    )

    t_sales_mp = PythonOperator(
        task_id='sales_mp_task',
        python_callable=_run_sales_mp,
    )

    # Clean Pipeline Flow
    (
        t_sales_2026
        >> t_sales_bazar
        >> t_sales_consinetto
        >> t_stock_parquet
        >> t_stock_and_sales
        >> t_count_artikel
        >> t_sales_ptsbz
        >> t_sales_mp
    )
