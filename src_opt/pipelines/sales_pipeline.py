"""
src_opt.pipelines.sales_pipeline
Independent, high-performance sales data extraction, transformation, and loading.
Encapsulates sales_to_sql, bazar, consinetto, margin calculation, and article counts.
Zero dependencies on legacy main.py.
"""
import gc
from datetime import datetime
import numpy as np
import pandas as pd

from src_opt.config.settings import settings
from src_opt.config.database import get_engine, get_pooled_engine
from src_opt.config.logger import get_pipeline_logger
from src_opt.common.utils import execution_timer
from src_opt.common.db_helper import safe_to_sql, add_index_and_foreign_keys

logger = get_pipeline_logger("src_opt.pipelines.sales")


def run_disc2nd_consi_online() -> None:
    """Calculate secondary discount for consignment and online sales."""
    with execution_timer("disc2nd_consi_online"):
        from modules.queries import df__sales_consi_disc2nd, df__sales_online_disc2nd

        logger.info("Extracting secondary discounts (consignment & online)...")
        df_consi = df__sales_consi_disc2nd()
        df_online = df__sales_online_disc2nd()
        df_combined = pd.concat([df_consi, df_online], ignore_index=True)

        if df_combined.empty:
            logger.warning("No secondary discount records found.")
            return

        agg_dict = {col: 'sum' for col in df_combined.columns if col not in ['fv_nopenjualan', 'qty']}
        agg_dict['qty'] = 'first'
        df_grouped = df_combined.groupby('fv_nopenjualan', as_index=False).agg(agg_dict)

        df_grouped['artsize_disc2nd'] = np.where(df_grouped['fn_potonganharga'] == 0, 0, (df_grouped['fn_potonganharga'] / df_grouped['qty']).round(2))
        df_grouped['artsize_servisfee'] = np.where(df_grouped['servisfee'] == 0, 0, (df_grouped['servisfee'] / df_grouped['qty']).round(2))
        df_grouped['artsize_premi'] = np.where(df_grouped['premi'] == 0, 0, (df_grouped['premi'] / df_grouped['qty']).round(2))
        df_grouped['artsize_affiliate'] = np.where(df_grouped['affiliate'] == 0, 0, (df_grouped['affiliate'] / df_grouped['qty']).round(2))
        df_grouped['artsize_voucher'] = np.where(df_grouped['voucher'] == 0, 0, (df_grouped['voucher'] / df_grouped['qty']))
        df_grouped.replace([np.inf, -np.inf], 0, inplace=True)
        df_grouped = df_grouped.fillna(0)

        safe_to_sql(df_grouped, 'disc2artsize_pb', stat='imp', if_exists='replace')
        gc.collect()


def run_margin_tb(year: int = None) -> None:
    """Process margin table for specific year."""
    if year is None:
        year = datetime.now().year

    with execution_timer(f"margin_tb_{year}"):
        from modules.queries import fetch_data_margin
        from modules.proses import process_data_margin

        logger.info(f"Processing margin table for year {year}...")
        df_trx, df_artimp, df_artred, df_atrxu, df_mrgincat, df_mrgin = fetch_data_margin(year)
        df_trx_final = process_data_margin(df_trx, df_artimp, df_artred, df_atrxu, df_mrgincat, df_mrgin)

        table_name = f'margin{str(year)[-2:]}_pb'
        safe_to_sql(df_trx_final, table_name, stat='imp', if_exists='replace')
        gc.collect()


def run_sales_to_sql(year: int = None) -> None:
    """
    Main sales ETL: extract sales & bazar, apply secondary discounts, margins,
    calculate nett and dpp, and save to pb_penjualan_{year}.
    """
    if year is None:
        year = datetime.now().year

    with execution_timer(f"sales_to_sql_{year}"):
        from modules.queries import df__penju, df__penju_bz, df_artikel_pb_
        from modules.proses import (
            clean_and_transform_data, insert_disc2_grouping, insert_margin,
            rename_col_py_to_sql, insert_pk, add_ornal
        )

        logger.info(f"Starting sales_to_sql for year {year}...")
        # Step 1: Pre-calculate secondary discount
        run_disc2nd_consi_online()

        # Step 2: Extract sales and bazar
        df1 = df__penju(year)
        df_bz = df__penju_bz(year)
        df_bz['fn_idurut'] += 100000000

        df = pd.concat([df1, df_bz], ignore_index=True)
        df = clean_and_transform_data(df)
        df = insert_disc2_grouping(df, 'sum')
        df = insert_margin(df, year)

        sum_col = ['fn_jualpersize', 'total_penjualan2', 'total_disc2']
        agg_col = ['fn_whconsiid', 'fv_artsizecode', 'tgltrx']
        first_col = [col for col in df.columns if col not in sum_col + agg_col]
        df = df[first_col + [col for col in df.columns if col not in first_col]]

        df['event'] = df['event'].fillna(df['status_artikel'])
        df['fn_margin'] = df['fn_margin'].fillna(0)
        df = rename_col_py_to_sql(df).sort_values(by='fn_idurut')
        df = insert_pk(df)

        df['fn_nett_amount'] = (df['fn_totalpenjualan'] - df['fn_total_disc2']) * (1 - df['fn_margin'] / 100)
        df['fn_sim_amount'] = np.where(df['fc_status'] == "X", df['fn_totalpenjualan'], df['fn_nett_amount'])
        df['fn_dpp'] = df['fn_nett_amount'] / 1.11
        df['fn_dpp2'] = df['fn_sim_amount'] / 1.11

        df_art = df_artikel_pb_('fv_artsizecode, fv_catname', distinct=False, where='')
        df = df.merge(df_art, how='left', on='fv_artsizecode')
        df = df.rename(columns={'fn_hargasatuan': 'fm_amount', 'fv_status_artikel': 'fv_status'})
        df['fv_ornal'] = df.apply(add_ornal, axis=1)
        df = df.rename(columns={'fm_amount': 'fn_hargasatuan', 'fv_status': 'fv_status_artikel'}).drop(columns=['fv_catname'], errors='ignore')

        table_name = f"pb_penjualan_{year}"
        safe_to_sql(df, table_name, stat='imp', if_exists='replace')

        add_index_and_foreign_keys(
            stat='imp',
            table_name=table_name,
            index_cols=["fn_whconsiid", "fv_artsizecode"],
            foreign_keys=[
                {
                    "column": "fn_whconsiid",
                    "ref_table": "customers_pb",
                    "ref_column": "fn_whconsiid",
                    "constraint_name": f"fk_whconsiid_pb_sales_{year}"
                },
                {
                    "column": "fv_artsizecode",
                    "ref_table": "artikel_pb",
                    "ref_column": "fv_artsizecode",
                    "constraint_name": f"fk_sales_artsize_pb_{year}"
                }
            ]
        )
        gc.collect()


def run_sales_bazar() -> None:
    """Transform and load bazar sales to pb_penjualanbz."""
    with execution_timer("sales_bazar"):
        from modules.queries import df__sales_bz_disc2nd, df__sales_bz
        from modules.proses import clean_and_transform_data, rename_col_py_to_sql

        logger.info("Processing bazar sales...")
        df_bz_disc2 = df__sales_bz_disc2nd()
        df_bz = df__sales_bz()
        df_bz = pd.merge(df_bz, df_bz_disc2, how="left", on='fv_nopenjualan').fillna(0)
        df_bz = clean_and_transform_data(df_bz)
        df_bz = rename_col_py_to_sql(df_bz).sort_values(by='fn_idurut')

        safe_to_sql(df_bz, 'pb_penjualanbz', stat='imp', if_exists='replace')
        gc.collect()


def run_sales_consinetto() -> None:
    """Transform and load consinetto sales to pb_penjualanpts."""
    with execution_timer("sales_consinetto"):
        from modules.queries import df__sales_invoice
        from modules.proses import sls_disctok, rename_col_py_to_sql

        logger.info("Processing consinetto sales...")
        df_si = df__sales_invoice()
        df_sidisctok = sls_disctok()
        df_sii = pd.merge(df_si, df_sidisctok, how="left", on='fc_nodoc').fillna(0)
        df_sii['fin_dpp'] = df_sii['dpp'] - df_sii['disctok']
        df_sii['total_amount'] = df_sii['fin_dpp'] * df_sii['fn_qty']
        df_sii = rename_col_py_to_sql(df_sii).sort_values(by='fv_nodoc')

        safe_to_sql(df_sii, 'pb_penjualanpts', stat='imp', if_exists='replace')
        gc.collect()


def run_sales_ptsbz() -> None:
    """Load PTS Bazar sales to penjualanbzrpts_pb."""
    with execution_timer("sales_ptsbz"):
        from modules.queries import df__sales_ptsbz

        df_si = df__sales_ptsbz()
        safe_to_sql(df_si, 'penjualanbzrpts_pb', stat='imp', if_exists='replace')
        gc.collect()


def run_sales_mp() -> None:
    """Load marketplace sales to penjualan_MrkPic_pb."""
    with execution_timer("sales_mp"):
        from modules.queries import df__sales_pmse

        df_si = df__sales_pmse()
        safe_to_sql(df_si, 'penjualan_MrkPic_pb', stat='imp', if_exists='replace')
        gc.collect()


def run_count_artikel() -> None:
    """Compute distinct active article counts per store and save to pb_count_artikel."""
    with execution_timer("count_artikel"):
        from modules.queries import df__sales_thru_25_count_artikel

        today_str = datetime.today().strftime("%Y-%m-%d")
        sales_thru_table = f"sales_thru_pb_{today_str[2:4]}"
        start_count = 1
        end_count = int(today_str[5:7])

        stocks = [f"fn_stock_{i:02d}" for i in range(start_count, end_count + 1)]
        stock_cols_str = ", ".join(stocks) + ", fn_stock_at"

        logger.info(f"Querying count artikel from {sales_thru_table}...")
        df = df__sales_thru_25_count_artikel(stock_cols_str, sales_thru_table)
        df = df.groupby(['fn_whconsiid', 'fv_barcode']).sum().reset_index()

        col_art = []
        for i in range(start_count, end_count + 1):
            stock_col = f'fn_stock_{i:02d}'
            art_col = f'fn_art_{i:02d}'
            df[art_col] = (df[stock_col] > 1).astype(int)
            col_art.append(art_col)

        df['fn_art_at'] = df['fn_stock_at'] > 1
        df = df[['fn_whconsiid', 'fv_barcode'] + col_art + ['fn_art_at']]

        safe_to_sql(df, 'pb_count_artikel', stat='imp', if_exists='replace')
        gc.collect()
