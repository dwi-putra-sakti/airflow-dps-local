"""
dags_opt.dag_manual_operations
Consolidated Manual / On-Demand Operations DAG.
Consolidates one-off tasks (sales_bazar, sales_consinetto, disc2nd, sales_to_sql, artsize_md, penjualan_table)
into a single, clean on-demand DAG with UI parameters.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
import pendulum

LOCAL_TZ = pendulum.timezone("Asia/Jakarta")

DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def _choose_operation(**context):
    """Branch based on selected operation parameter in Airflow UI."""
    op = context['params'].get('operation', 'all')
    valid_tasks = {
        'sales_to_sql': 'op_sales_to_sql',
        'sales_bazar': 'op_sales_bazar',
        'sales_consinetto': 'op_sales_consinetto',
        'disc2nd': 'op_disc2nd',
        'md_artikel_size': 'op_md_artikel_size',
        'penjualan_by_table': 'op_penjualan_by_table',
    }
    if op in valid_tasks:
        return valid_tasks[op]
    # Return all tasks if 'all' is selected
    return list(valid_tasks.values())


# Task wrappers
def _exec_sales_to_sql(**context):
    year = context['params'].get('year', 2026)
    from src_opt.facade import task_sales_to_sql
    return task_sales_to_sql(year=year, **context)


def _exec_sales_bazar(**context):
    from src_opt.facade import task_sales_bazar
    return task_sales_bazar(**context)


def _exec_sales_consinetto(**context):
    from src_opt.facade import task_sales_consinetto
    return task_sales_consinetto(**context)


def _exec_disc2nd(**context):
    from src_opt.facade import task_disc2nd_consi_online
    return task_disc2nd_consi_online(**context)


def _exec_md_artsize(**context):
    from src_opt.facade import task_md_artikel_size
    return task_md_artikel_size(**context)


def _exec_penjualan_table(**context):
    year = context['params'].get('year', 2026)
    from src_opt.facade import task_penjualan_by_table
    return task_penjualan_by_table(year=year, **context)


with DAG(
    dag_id='opt_manual_operations',
    default_args=DEFAULT_ARGS,
    description='[OPTIMIZED] Trigger Manual Terpadu untuk Operasi Ad-Hoc / One-Run',
    start_date=datetime(2024, 1, 1, tzinfo=LOCAL_TZ),
    schedule_interval=None,
    catchup=False,
    params={
        'operation': Param(
            'all',
            type='string',
            enum=['all', 'sales_to_sql', 'sales_bazar', 'sales_consinetto', 'disc2nd', 'md_artikel_size', 'penjualan_by_table'],
            description='Pilih operasi yang ingin dijalankan'
        ),
        'year': Param(2026, type='integer', description='Tahun target (untuk sales_to_sql dan penjualan_by_table)')
    },
    tags=['opt', 'manual', 'adhoc', 'one-run']
) as dag:

    branch_task = BranchPythonOperator(
        task_id='select_operation',
        python_callable=_choose_operation,
    )

    t_sales_sql = PythonOperator(
        task_id='op_sales_to_sql',
        python_callable=_exec_sales_to_sql,
    )

    t_sales_bz = PythonOperator(
        task_id='op_sales_bazar',
        python_callable=_exec_sales_bazar,
    )

    t_sales_cs = PythonOperator(
        task_id='op_sales_consinetto',
        python_callable=_exec_sales_consinetto,
    )

    t_disc2nd = PythonOperator(
        task_id='op_disc2nd',
        python_callable=_exec_disc2nd,
    )

    t_artsize = PythonOperator(
        task_id='op_md_artikel_size',
        python_callable=_exec_md_artsize,
    )

    t_penjualan_tbl = PythonOperator(
        task_id='op_penjualan_by_table',
        python_callable=_exec_penjualan_table,
    )

    branch_task >> [t_sales_sql, t_sales_bz, t_sales_cs, t_disc2nd, t_artsize, t_penjualan_tbl]
