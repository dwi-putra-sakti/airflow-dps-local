"""Pipelines package for src_opt."""
import importlib
from typing import Any

# Mapping of pipeline function names to their respective module files.
# Lazily resolved via PEP 562 __getattr__ so importing one pipeline does not load all others into memory.
_EXPORT_MAP = {
    # Sales pipeline
    "run_sales_to_sql": "sales_pipeline",
    "run_sales_bazar": "sales_pipeline",
    "run_sales_consinetto": "sales_pipeline",
    "run_sales_ptsbz": "sales_pipeline",
    "run_sales_mp": "sales_pipeline",
    "run_count_artikel": "sales_pipeline",
    "run_disc2nd_consi_online": "sales_pipeline",
    "run_margin_tb": "sales_pipeline",
    # Stock pipeline
    "run_stock_daily": "stock_pipeline",
    "run_get_last_three_months_including_current": "stock_pipeline",
    "run_stock_and_sales": "stock_pipeline",
    "run_fstok_gudang": "stock_pipeline",
    "run_stock_by_artikel": "stock_pipeline",
    "run_stock_borken_aging": "stock_pipeline",
    # Master pipeline
    "run_upsert_artikel_pb": "master_pipeline",
    "run_upsert_customers_pb": "master_pipeline",
    "run_upsert_tgl_so_sa": "master_pipeline",
    "run_upsert_whtype": "master_pipeline",
    "run_upsert_master": "master_pipeline",
    "run_aging_gudang_pb": "master_pipeline",
    "run_upsert_expedition_tracking": "master_pipeline",
    # Reports pipeline
    "run_mini_consigment_sales": "reports_pipeline",
    "run_ost_ob": "reports_pipeline",
    "run_ost_do": "reports_pipeline",
    "run_master_gudang": "reports_pipeline",
    "run_md_artikel_size": "reports_pipeline",
    "run_md_artikel_size2": "reports_pipeline",
    "run_md_artikel_size_26": "reports_pipeline",
    "run_md_status_artikel": "reports_pipeline",
    "run_penjualan_by_table": "reports_pipeline",
}

__all__ = list(_EXPORT_MAP.keys())


def __getattr__(name: str) -> Any:
    if name in _EXPORT_MAP:
        module = importlib.import_module(f".{_EXPORT_MAP[name]}", package=__package__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

