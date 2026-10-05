"""
src_opt.pipelines.reports_pipeline
Independent, high-performance report and Excel generation pipeline.
Generates store aging, consignment summaries, outstanding OB/DO, and exports to SMB share.
Zero dependencies on legacy main.py.
"""
import os
import gc
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from typing import Optional
import numpy as np
import pandas as pd

from src_opt.config.settings import settings
from src_opt.config.database import get_engine, get_pooled_engine
from src_opt.config.logger import get_pipeline_logger
from src_opt.common.utils import execution_timer

logger = get_pipeline_logger("src_opt.pipelines.reports")


def run_mini_consigment_sales(year: int = None) -> None:
    """Generate mini consignment sales Excel report."""
    if year is None:
        year = datetime.now().year

    with execution_timer(f"mini_consigment_sales_{year}"):
        from modules.queries import df__sales_, df__artikel_pb, df__customers_pb, df__tgl_input_penju
        from modules.proses import rename_col_sql_to_xlsx_gis

        logger.info(f"Extracting sales and master data for mini consignment {year}...")
        df = df__sales_(year, 'fn_whconsiid,fv_artsizecode,fv_event, fd_tgltrx,fn_jualpersize,fn_totalpenjualan,fn_nett_amount,fv_nopenjualan,fn_tabel,fn_total_disc2')
        df_artpb = df__artikel_pb('fv_artsizecode,fv_barcode,fv_catname,fv_fitname,fv_brandname,fv_divname,fv_sizename,fm_price,fv_ornal,fv_status,fv_colorname,fv_fitting,motif,fv_namemark')
        df_cuspb = df__customers_pb('fv_toko,fc_code,fn_whconsiid,fc_detail_tok Group_toko')

        df = pd.merge(df, df_artpb, on='fv_artsizecode', how='left')
        df = pd.merge(df, df_cuspb, on='fn_whconsiid', how='left')

        cols = [
            'fd_tgltrx', 'fc_code', 'fv_toko', 'Group_toko', 'fv_barcode', 'fv_catname', 'fv_colorname',
            'fv_fitting', 'motif', 'fv_brandname', 'fv_nopenjualan', 'fv_divname', 'fv_sizename',
            'fv_event', 'fm_price', 'fv_ornal', 'fv_status', 'fn_totalpenjualan', 'fn_nett_amount',
            'fn_jualpersize', 'fv_namemark', 'fn_tabel', 'fn_total_disc2'
        ]
        df = df[cols]
        df = rename_col_sql_to_xlsx_gis(df)

        df['Sales Date'] = pd.to_datetime(df['Sales Date'])
        df['bulan_num'] = df['Sales Date'].dt.strftime('%m')
        df['bulan'] = df['Sales Date'].dt.strftime('%b')
        df['tahun'] = df['Sales Date'].dt.strftime('%y')

        df_tglimput = df__tgl_input_penju(year)
        df = df.merge(df_tglimput, how='left', on='fv_nopenjualan')
        df[['No. Pj', 'Status Master']] = df[['fv_nopenjualan', 'fv_namemark']]

        output_folder = os.path.join(settings.SMB_PATH, "For Alokator", "Sales cosiment", str(year))
        os.makedirs(output_folder, exist_ok=True)
        out_path = os.path.join(output_folder, f"Mini Consignment Sales {year}.xlsx")

        # Save with memory chunking if huge
        df.to_excel(out_path, index=False)
        logger.info(f"Saved mini consignment report to: {out_path}")
        gc.collect()


def run_ost_ob() -> None:
    """Process outstanding OB and export to Excel."""
    with execution_timer("ost_ob"):
        from modules.queries import df__customers_pb_, df_artikel_pb_, df__user, df__otsob

        logger.info("Processing outstanding OB...")
        df_cus = df__customers_pb_('fn_cusid,fc_code "Code Toko",fv_toko Toko', distinct=False, where='')
        df_art = df_artikel_pb_('fv_artsizecode, fv_barcode "Artikel", fv_sizename "Size"', distinct=False, where='')
        df_user = df__user('fn_userid,fv_username "User"')
        df_ostob = df__otsob()

        df_ostob = df_art.merge(df_ostob, how='right', on='fv_artsizecode').drop(columns=['fv_artsizecode'], errors='ignore')
        df_ostob = df_cus.merge(df_ostob, how='right', on='fn_cusid').drop(columns=['fn_cusid'], errors='ignore')
        df_ostob = df_ostob.merge(df_user, how='left', on='fn_userid').drop(columns=['fn_userid'], errors='ignore')

        out_dir = os.path.join(settings.SMB_PATH, "For Alokator")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "Out Standing OB.xlsx")
        df_ostob.to_excel(out_path, index=False)
        logger.info(f"Saved outstanding OB to: {out_path}")
        gc.collect()


def run_ost_do() -> None:
    """Process outstanding DO and export to Excel."""
    with execution_timer("ost_do"):
        from modules.queries import df__customers_pb_, df_artikel_pb_, df__user, df__otsdo_rt

        logger.info("Processing outstanding DO...")
        df_cus = df__customers_pb_('fn_cusid,fc_code "Code Toko",fv_toko Toko', distinct=False, where='')
        df_art = df_artikel_pb_('fv_artsizecode, fv_barcode "Artikel", fv_sizename "Size"', distinct=False, where='')
        df_user = df__user('fn_userid,fv_username "User"')

        input_value = (date.today().replace(day=1) - relativedelta(months=1)).strftime("%Y-%m-%d")
        df_ostdo = df__otsdo_rt(input_value).drop(columns=['fc_status'], errors='ignore')

        df_ostdo = df_art.merge(df_ostdo, how='right', on='fv_artsizecode').drop(columns=['fv_artsizecode'], errors='ignore')
        df_ostdo = df_cus.merge(df_ostdo, how='right', on='fn_cusid').drop(columns=['fn_cusid'], errors='ignore')
        df_ostdo = df_ostdo.merge(df_user, how='left', on='fn_userid').drop(columns=['fn_userid'], errors='ignore')

        out_dir = os.path.join(settings.SMB_PATH, "For Alokator")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "Out Standing DO.xlsx")
        df_ostdo.to_excel(out_path, index=False)
        logger.info(f"Saved outstanding DO to: {out_path}")
        gc.collect()


def run_master_gudang() -> None:
    """Generate warehouse master summary report."""
    with execution_timer("master_gudang"):
        from modules.queries import df__stock_wh, df_artikel_pb_

        logger.info("Generating master gudang report...")
        df = df__stock_wh()
        df_artsize = df_artikel_pb_('fv_barcode,fn_articleid,fv_artsizecode,fv_sizename,fv_configname')
        df = df.merge(df_artsize, how='left', on='fv_artsizecode')

        df_piv = pd.pivot_table(
            df,
            values='fn_qty',
            index=['fv_barcode', 'fn_articleid', 'fv_configname'],
            columns='fv_sizename',
            aggfunc='sum',
            fill_value=0
        ).reset_index()

        out_dir = os.path.join(settings.SMB_PATH, "For Alokator")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "Data Master Gudang.xlsx")
        df_piv.to_excel(out_path, index=False)
        logger.info(f"Saved master gudang report to: {out_path}")
        gc.collect()


def run_md_status_artikel() -> None:
    """Generate article status report."""
    with execution_timer("md_status_artikel"):
        from modules.queries import df__artsizecode, df__sales_thru_26
        from modules.proses import proc_pivot_table_n

        logger.info("Generating status artikel report...")
        df_artsq = df__artsizecode(col='fv_artsizecode, fv_barcode, fv_sizename, fv_configname', stat='imp')
        df = df__sales_thru_26('fv_artsizecode,fn_whconsiid,fn_stock_at')
        df = df.merge(df_artsq, on='fv_artsizecode', how='left')

        df = proc_pivot_table_n(
            df,
            values='fn_stock_at',
            col_index=['fv_barcode', 'fn_whconsiid'],
            columns=['fv_sizename'],
            col_ket=['fv_configname']
        )
        df = df[['fv_barcode', 'fn_whconsiid', 'status']].drop_duplicates()

        out_dir = os.path.join(settings.SMB_PATH, "For MD")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "status_artikel.xlsx")
        df.to_excel(out_path, index=False)
        logger.info(f"Saved status artikel report to: {out_path}")
        gc.collect()


def run_md_artikel_size() -> None:
    """Generate article size markdown report for 2025."""
    with execution_timer("md_artikel_size_25"):
        from modules.queries import df__cus, df__artsizecode, df__sales_thru_25
        from modules.proses import generate_rename_dict

        logger.info("Generating md_artikel_size (2025)...")
        df_cus = df__cus()
        df_artsq = df__artsizecode(col='fv_artsizecode, fv_barcode, fv_sizename, fv_configname', stat='imp')
        df_sq25 = df__sales_thru_25('*')

        df_sq25 = pd.merge(df_sq25, df_artsq, on='fv_artsizecode', how='left', copy=False)
        df_sq25 = pd.merge(df_sq25, df_cus, on='fn_whconsiid', how='left', copy=False)

        df_sq25['fn_artsizeid'] = pd.to_numeric(df_sq25['fn_artsizeid'], downcast='integer')
        df_sq25['pk_id'] = df_sq25['fn_whconsiid'].astype(str) + "/" + df_sq25['fn_artsizeid'].astype(str)
        df_sq25 = df_sq25.drop(columns=['fn_cusid'], errors='ignore')

        df_sq25 = generate_rename_dict(2024, 2025, df_sq25)
        out_dir = os.path.join(settings.SMB_PATH, "for MD")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "stock artikel all store 2025.xlsx")
        df_sq25.to_excel(out_path, index=False)
        logger.info(f"Saved md_artikel_size to: {out_path}")
        gc.collect()


def run_md_artikel_size_26() -> None:
    """Generate article size report for 2026."""
    with execution_timer("md_artikel_size_26"):
        from modules.queries import df__cus, df__artsizecode, df__sales_thru_26
        from modules.proses import generate_rename_dict

        logger.info("Generating md_artikel_size_26...")
        df_cus = df__cus()
        df_artsq = df__artsizecode(col='fv_artsizecode, fv_barcode, fv_sizename, fv_configname', stat='imp')
        df_sq26 = df__sales_thru_26('*')

        # Memory optimization: category downcasting
        for col in ['fv_artsizecode', 'fn_whconsiid']:
            if col in df_sq26.columns:
                df_sq26[col] = df_sq26[col].astype('category')

        for col in ['fv_artsizecode', 'fv_barcode', 'fv_sizename', 'fv_configname']:
            if col in df_artsq.columns:
                df_artsq[col] = df_artsq[col].astype('category')

        df_sq26 = pd.merge(df_sq26, df_artsq, on='fv_artsizecode', how='left', copy=False)
        df_sq26 = pd.merge(df_sq26, df_cus, on='fn_whconsiid', how='left', copy=False)

        df_sq26['fn_artsizeid'] = pd.to_numeric(df_sq26['fn_artsizeid'], downcast='integer')
        df_sq26['pk_id'] = df_sq26['fn_whconsiid'].astype(str) + "/" + df_sq26['fn_artsizeid'].astype(str)
        df_sq26 = df_sq26.drop(columns=['fn_cusid'], errors='ignore')

        df_sq26 = generate_rename_dict(2025, 2026, df_sq26)
        out_dir = os.path.join(settings.SMB_PATH, "for MD")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "stock artikel all store 2026.xlsx")
        df_sq26.to_excel(out_path, index=False)
        logger.info(f"Saved md_artikel_size_26 to: {out_path}")
        gc.collect()


def run_md_artikel_size2() -> None:
    """Generate secondary article size summary report."""
    with execution_timer("md_artikel_size2"):
        from modules.proses import generate_rename_dict
        with get_engine('imp') as engine_imp:
            query_cus = "SELECT fc_code, fv_toko, fn_whconsiid FROM gis_local.customers_pb"
            df_cus = pd.read_sql(query_cus, engine_imp)

            query_art = "SELECT fv_barcode artikel, fv_artsizecode barcode, fv_sizename ukuran FROM artikel_pb"
            df_artsq = pd.read_sql(query_art, engine_imp)

            query_sq24 = "SELECT * FROM gis_local.sales_thru_pb_24"
            df_sq24 = pd.read_sql(query_sq24, engine_imp)

            query_sq25 = "SELECT * FROM gis_local.sales_thru_pb_25"
            df_sq25 = pd.read_sql(query_sq25, engine_imp)

        df_sq24 = pd.merge(df_sq24, df_artsq, left_on='fv_artsizecode', right_on='barcode', how='left')
        df_sq24 = pd.merge(df_sq24, df_cus, on='fn_whconsiid', how='left')
        df_sq24['pk_id'] = df_sq24['fn_whconsiid'].astype(str) + "/" + df_sq24['fn_artsizeid'].astype(str)

        df_sq25 = pd.merge(df_sq25, df_artsq, left_on='fv_artsizecode', right_on='barcode', how='left')
        df_sq25 = pd.merge(df_sq25, df_cus, on='fn_whconsiid', how='left')
        df_sq25['pk_id'] = df_sq25['fn_whconsiid'].astype(str) + "/" + df_sq25['fn_artsizeid'].astype(str)

        out_dir = os.path.join(settings.SMB_PATH, "for MD")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "stock artikel all store 2026(2).xlsx")
        df_sq25.to_excel(out_path, index=False)
        logger.info(f"Saved md_artikel_size2 to: {out_path}")
        gc.collect()


def run_penjualan_by_table(year: int = None) -> None:
    """Generate yearly sales summary by table."""
    if year is None:
        year = datetime.now().year

    with execution_timer(f"penjualan_by_table_{year}"):
        from modules.proses import get_penjualan_tabel

        logger.info(f"Generating penjualan by table for {year}...")
        get_penjualan_tabel(year)
        gc.collect()
