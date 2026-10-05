"""
src_opt.facade
Clean facade and entry points for Airflow DAG tasks.
Ensures lightweight execution, deferred imports, standardized logging, and error handling.
"""
import sys
import os
from pathlib import Path

# Ensure paths are discoverable in both Docker and local environment
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SRC_PATH = _PROJECT_ROOT / "src"
_SRC_OPT_PATH = _PROJECT_ROOT / "src_opt"

for p in [str(_SRC_OPT_PATH), str(_SRC_PATH), str(_PROJECT_ROOT), "/opt/airflow/src", "/opt/airflow/src_opt"]:
    if p not in sys.path and (os.path.exists(p) or p.startswith("/opt")):
        sys.path.insert(0, p)

from src_opt.config.logger import get_pipeline_logger

logger = get_pipeline_logger("src_opt.facade")

# --- Stock Facades ---
def task_stock_daily(force: bool = False, **context):
    """Entry point for real-time stock sync task with smart change detection."""
    from src_opt.pipelines.stock_pipeline import run_stock_daily
    if not force and context:
        force = context.get('params', {}).get('force', False)
    return run_stock_daily(force=force, skip_on_no_change=True)



def task_stock_monthly(typefile: str = "parquet", **context):
    """Entry point for rolling 3-month stock sync."""
    from src_opt.pipelines.stock_pipeline import run_get_last_three_months_including_current
    return run_get_last_three_months_including_current(typefile=typefile)


def task_stock_and_sales(start_year: int = 2026, end_year: int = 2026, typefile: str = "parquet", **context):
    """Entry point for merging stock and sales."""
    from src_opt.pipelines.stock_pipeline import run_stock_and_sales
    return run_stock_and_sales(start_year, end_year, typefile=typefile)


# --- Sales Facades ---
def task_sales_to_sql(year: int = 2026, **context):
    """Entry point for main sales to SQL pipeline."""
    from src_opt.pipelines.sales_pipeline import run_sales_to_sql
    return run_sales_to_sql(year)


def task_sales_bazar(**context):
    """Entry point for bazar sales."""
    from src_opt.pipelines.sales_pipeline import run_sales_bazar
    return run_sales_bazar()


def task_sales_consinetto(**context):
    """Entry point for consinetto sales."""
    from src_opt.pipelines.sales_pipeline import run_sales_consinetto
    return run_sales_consinetto()


def task_sales_ptsbz(**context):
    """Entry point for PTS Bazar sales."""
    from src_opt.pipelines.sales_pipeline import run_sales_ptsbz
    return run_sales_ptsbz()


def task_sales_mp(**context):
    """Entry point for Marketplace sales."""
    from src_opt.pipelines.sales_pipeline import run_sales_mp
    return run_sales_mp()


def task_count_artikel(**context):
    """Entry point for article count aggregation."""
    from src_opt.pipelines.sales_pipeline import run_count_artikel
    return run_count_artikel()


def task_disc2nd_consi_online(**context):
    """Entry point for secondary discount."""
    from src_opt.pipelines.sales_pipeline import run_disc2nd_consi_online
    return run_disc2nd_consi_online()


# --- Master Upsert Facades ---
def task_upsert_artikel(**context):
    from src_opt.pipelines.master_pipeline import run_upsert_artikel_pb
    return run_upsert_artikel_pb()


def task_upsert_customers(**context):
    from src_opt.pipelines.master_pipeline import run_upsert_customers_pb
    return run_upsert_customers_pb()


def task_upsert_tgl_so_sa(**context):
    from src_opt.pipelines.master_pipeline import run_upsert_tgl_so_sa
    return run_upsert_tgl_so_sa()


def task_upsert_whtype(**context):
    from src_opt.pipelines.master_pipeline import run_upsert_whtype
    return run_upsert_whtype()


def task_upsert_master(**context):
    from src_opt.pipelines.master_pipeline import run_upsert_master
    return run_upsert_master()


def task_aging_gudang(**context):
    from src_opt.pipelines.master_pipeline import run_aging_gudang_pb
    return run_aging_gudang_pb()


def task_upsert_expedition_tracking(**context):
    from src_opt.pipelines.master_pipeline import run_upsert_expedition_tracking
    return run_upsert_expedition_tracking()


# --- Reports Facades ---
def task_mini_consigment_sales(year: int = 2026, **context):
    from src_opt.pipelines.reports_pipeline import run_mini_consigment_sales
    return run_mini_consigment_sales(year)


def task_ost_ob(**context):
    from src_opt.pipelines.reports_pipeline import run_ost_ob
    return run_ost_ob()


def task_ost_do(**context):
    from src_opt.pipelines.reports_pipeline import run_ost_do
    return run_ost_do()


def task_master_gudang(**context):
    from src_opt.pipelines.reports_pipeline import run_master_gudang
    return run_master_gudang()


def task_stock_borken_aging(**context):
    from src_opt.pipelines.stock_pipeline import run_stock_borken_aging
    return run_stock_borken_aging()


def task_fstok_gudang(**context):
    from src_opt.pipelines.stock_pipeline import run_fstok_gudang
    return run_fstok_gudang()


def task_stock_by_artikel(**context):
    from src_opt.pipelines.stock_pipeline import run_stock_by_artikel
    return run_stock_by_artikel()


def task_md_status_artikel(**context):
    from src_opt.pipelines.reports_pipeline import run_md_status_artikel
    return run_md_status_artikel()


def task_penjualan_by_table(year: int = 2026, **context):
    from src_opt.pipelines.reports_pipeline import run_penjualan_by_table
    return run_penjualan_by_table(year)


def task_md_artikel_size(**context):
    from src_opt.pipelines.reports_pipeline import run_md_artikel_size
    return run_md_artikel_size()


def task_md_artikel_size2(**context):
    from src_opt.pipelines.reports_pipeline import run_md_artikel_size2
    return run_md_artikel_size2()


def task_md_artikel_size_26(**context):
    from src_opt.pipelines.reports_pipeline import run_md_artikel_size_26
    return run_md_artikel_size_26()
