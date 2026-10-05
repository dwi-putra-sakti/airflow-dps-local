"""
src_opt.pipelines.stock_pipeline
Independent, high-performance stock synchronization and parquet pipeline.
Replaces O(N^2) DataFrame concatenation loops with single bulk operations.
Zero dependencies on legacy main.py.
"""
import os
import gc
import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Optional, List
import numpy as np
import pandas as pd

from src_opt.config.settings import settings
from src_opt.config.database import get_engine, get_pooled_engine
from src_opt.config.logger import get_pipeline_logger
from src_opt.common.utils import execution_timer
from src_opt.common.db_helper import safe_to_sql, add_index_and_foreign_keys

logger = get_pipeline_logger("src_opt.pipelines.stock")


def run_stock_daily(force: bool = False, skip_on_no_change: bool = True) -> bool:
    """
    Real-time stock sync executed every 2 minutes.
    Extracts real-time transactions from read DB and writes to write DB.
    Optimized: Skips database rewrite if transaction count and total stock have not changed today.
    """
    with execution_timer("stock_daily_realtime"):
        from modules.queries import df__rtt

        logger.info("Checking real-time stock transactions for today...")
        df_trxstx = df__rtt()
        if df_trxstx is None or df_trxstx.empty:
            logger.warning("No real-time stock records found.")
            return False

        src_cnt = len(df_trxstx)
        src_stock = float(df_trxstx['stock'].sum()) if src_cnt > 0 else 0.0

        if not force:
            try:
                with get_engine('imp') as eng:
                    dest_df = pd.read_sql('SELECT COUNT(*) as cnt, IFNULL(SUM(fn_stock), 0) as total_stock FROM trxrt_pb', eng)
                    dest_cnt = int(dest_df['cnt'].iloc[0])
                    dest_stock = float(dest_df['total_stock'].iloc[0])

                if src_cnt == dest_cnt and abs(src_stock - dest_stock) < 1e-4:
                    msg = f"⚡ [SKIP] Data transaksi hari ini belum berubah (Count: {src_cnt}, Stock: {src_stock}). Sinkronisasi dilewati."
                    logger.info(msg)
                    if skip_on_no_change:
                        try:
                            from airflow.exceptions import AirflowSkipException
                            raise AirflowSkipException(msg)
                        except ImportError:
                            return False
                    return False
            except Exception as e:
                # Re-raise AirflowSkipException so Airflow marks the task state as SKIPPED
                if e.__class__.__name__ == 'AirflowSkipException':
                    raise
                logger.debug(f"Could not compare destination table, proceeding with sync: {e}")

        logger.info(f"🔄 [SYNC] Terdeteksi perubahan data! Memproses {src_cnt} transaksi (Stock: {src_stock})...")
        df_trxstx.loc[df_trxstx['tgltrx'] == '10-30-0025', 'tgltrx'] = '10-30-2025'
        df_trxstx['tgltrx'] = pd.to_datetime(df_trxstx['tgltrx'], format='%m-%d-%Y')
        df_trxstx['fn_whconsiid'] = df_trxstx['fn_whconsiid'].astype(int)
        df_trxstx['stock'] = df_trxstx['stock'].astype(int)
        df_trxstx = df_trxstx.rename(columns={
            'stock': 'fn_stock',
            'tgltrx': 'fd_tgltrx',
            'stat': 'fv_stat'
        })

        safe_to_sql(df_trxstx, 'trxrt_pb', stat='imp', if_exists='replace')
        gc.collect()
        return True



def write_stock_parquet(df: pd.DataFrame, parquet_base: str, bz: bool = False) -> str:
    """Write stock DataFrame to partitioned parquet directory structure."""
    year = int(df['tahun'].iloc[0])
    month = int(df['bulan'].iloc[0])

    output_path = os.path.join(parquet_base, f"tahun={year}", f"bulan={str(month).zfill(2)}")
    os.makedirs(output_path, exist_ok=True)

    filename = "stock_bz.parquet" if bz else "stock_dept.parquet"
    output_file = os.path.join(output_path, filename)

    df.to_parquet(output_file, engine="pyarrow", index=False)
    logger.info(f"Parquet saved: {output_file}")
    return output_file


def merge_stock_by_date(tgl_input: str) -> Optional[str]:
    """Merge department and bazar stock parquet files for a given date."""
    try:
        dt = datetime.strptime(tgl_input, '%Y-%m-%d')
        next_month_dt = dt + relativedelta(months=1)
        year_str = str(next_month_dt.year)
        month_str = str(next_month_dt.month).zfill(2)

        base_input = os.path.join(settings.SMB_PATH, "PB DB", "stock")
        path_dept = os.path.join(base_input, f"tahun={year_str}", f"bulan={month_str}", "stock_dept.parquet")
        path_bz = os.path.join(base_input, f"tahun={year_str}", f"bulan={month_str}", "stock_bz.parquet")

        if not os.path.exists(path_dept) or not os.path.exists(path_bz):
            logger.warning(f"Parquet file not found for {year_str}/{month_str}")
            return None

        df_dept = pd.read_parquet(path_dept)
        df_bz = pd.read_parquet(path_bz)
        df_merged = pd.concat([df_dept, df_bz], ignore_index=True)

        output_path = os.path.join(base_input, f"tahun={year_str}", f"bulan={month_str}")
        os.makedirs(output_path, exist_ok=True)
        output_file = os.path.join(output_path, "stock.parquet")

        df_merged.to_parquet(output_file, engine="pyarrow", index=False)
        logger.info(f"Merged parquet saved: {output_file}")
        return output_file
    except Exception as exc:
        logger.error(f"Error merging stock parquet for {tgl_input}: {exc}")
        return None


def get_stock_monthly_parquet(tglEND: str, expot_excel: str) -> pd.DataFrame:
    """Calculate monthly department stock and save as parquet or excel."""
    from modules.queries import df__tokoawal, df_tglsosa, df__transaction

    tglEND2 = datetime.strptime(tglEND, "%Y-%m-%d").date()
    tglEND_str = datetime.strptime(tglEND, '%Y-%m-%d').strftime('%Y-%m-%d 23:59:59')

    year = int(tglEND[:4])
    month = int(tglEND[5:7])
    day = int(tglEND[8:10])
    last_day = calendar.monthrange(year, month)[1]
    if day == last_day:
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1
    bulan = calendar.month_name[month].lower()[:3]
    tglnow = datetime.today().date()

    df_tokoawal_res = df__tokoawal()
    idwhtoko_list_awal = df_tokoawal_res['fn_whconsiid'].tolist()

    df_tglLSO = df_tglsosa(idwhtoko_list_awal, tglEND_str)
    df_tglLSO['fd_tglflagIndo'] = pd.to_datetime(df_tglLSO['fd_tglflagIndo'])
    df_tglLSO = df_tglLSO.loc[~((df_tglLSO['fd_tglflagIndo'] < tglEND_str) & (df_tglLSO['fc_hold'] == 'T'))]

    if tglEND2 > tglnow:
        df_tglLSO.loc[df_tglLSO['fc_status'] == "T", 'fd_tglflagIndo'] -= pd.Timedelta(days=1)

    df_tglLSO['fd_tglflagIndo'] = df_tglLSO['fd_tglflagIndo'].dt.strftime('%Y-%m-%d')
    df_tglLSO = df_tglLSO.loc[~df_tglLSO['fd_tglflagIndo'].isna()]

    idwhtoko_list = df_tglLSO['fn_whconsiid'].tolist()
    tgl_list = df_tglLSO['fd_tglflagIndo'].tolist()
    fv_stat = df_tglLSO['fv_stat'].tolist()

    # O(N) list append optimization instead of O(N^2) concat in loop
    tok_frames: List[pd.DataFrame] = []
    for idwhtoko, tglSTR, stat in zip(idwhtoko_list, tgl_list, fv_stat):
        df_transactions = df__transaction(idwhtoko, tglSTR, tglEND_str, stat)
        if not df_transactions.empty:
            df_transactions['fv_artsizecode'] = df_transactions['fv_artsizecode'].str.upper().replace({'BBRD103V042333-': 'BBRD103V042333'})
            df_transactions['fn_whconsiid'] = idwhtoko
            df_transactions['tgltrx'] = tglEND_str
            tok_frames.append(df_transactions)

    df_tok = pd.concat(tok_frames, ignore_index=True) if tok_frames else pd.DataFrame(columns=['fn_whconsiid', 'fv_artsizecode', 'qty', 'tgltrx'])
    fdate = str(year)[2:4]
    df_tok = df_tok[['fn_whconsiid', 'fv_artsizecode', 'qty', 'tgltrx']].rename(columns={'qty': f'stock {bulan}_{fdate}'})
    df_tok['tahun'] = year
    df_tok['bulan'] = month

    if expot_excel == 'parquet':
        parquet_base = os.path.join(settings.SMB_PATH, "PB DB", "stock")
        write_stock_parquet(df_tok, parquet_base, bz=False)
    return df_tok


def get_stock_monthly_parquet_bz(tglEND: str, expot_excel: str) -> pd.DataFrame:
    """Calculate monthly bazar stock and save as parquet or excel."""
    from modules.queries import df__tokoawal, df_tglsosa_bz, df__transaction_bz

    tglEND2 = datetime.strptime(tglEND, "%Y-%m-%d").date()
    tglEND_str = datetime.strptime(tglEND, '%Y-%m-%d').strftime('%Y-%m-%d 23:59:59')

    year = int(tglEND[:4])
    month = int(tglEND[5:7])
    day = int(tglEND[8:10])
    last_day = calendar.monthrange(year, month)[1]
    if day == last_day:
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1
    bulan = calendar.month_name[month].lower()[:3]
    tglnow = datetime.today().date()

    df_tokoawal_res = df__tokoawal()
    idwhtoko_list_awal = df_tokoawal_res['fn_whconsiid'].tolist()

    df_tglLSO = df_tglsosa_bz(idwhtoko_list_awal, tglEND_str)
    df_tglLSO['fd_tglflagIndo'] = pd.to_datetime(df_tglLSO['fd_tglflagIndo'])
    df_tglLSO = df_tglLSO.loc[~((df_tglLSO['fd_tglflagIndo'] < tglEND_str) & (df_tglLSO['fc_hold'] == 'T'))]

    if tglEND2 > tglnow:
        df_tglLSO.loc[df_tglLSO['fc_status'] == "T", 'fd_tglflagIndo'] -= pd.Timedelta(days=1)

    df_tglLSO['fd_tglflagIndo'] = df_tglLSO['fd_tglflagIndo'].dt.strftime('%Y-%m-%d')
    df_tglLSO = df_tglLSO.loc[~df_tglLSO['fd_tglflagIndo'].isna()]

    idwhtoko_list = df_tglLSO['fn_whconsiid'].tolist()
    tgl_list = df_tglLSO['fd_tglflagIndo'].tolist()
    fv_stat = df_tglLSO['fv_stat'].tolist()

    # O(N) list append optimization
    tok_frames: List[pd.DataFrame] = []
    for idwhtoko, tglSTR, stat in zip(idwhtoko_list, tgl_list, fv_stat):
        df_transactions = df__transaction_bz(idwhtoko, tglSTR, tglEND_str, stat)
        if not df_transactions.empty:
            df_transactions['fv_artsizecode'] = df_transactions['fv_artsizecode'].str.upper().replace({'BBRD103V042333-': 'BBRD103V042333'})
            df_transactions['fn_whconsiid'] = idwhtoko
            df_transactions['tgltrx'] = tglEND_str
            tok_frames.append(df_transactions)

    df_tok = pd.concat(tok_frames, ignore_index=True) if tok_frames else pd.DataFrame(columns=['fn_whconsiid', 'fv_artsizecode', 'qty', 'tgltrx'])
    fdate = str(year)[2:4]
    df_tok = df_tok[['fn_whconsiid', 'fv_artsizecode', 'qty', 'tgltrx']].rename(columns={'qty': f'stock {bulan}_{fdate}'})
    df_tok['tahun'] = year
    df_tok['bulan'] = month

    if expot_excel == 'parquet':
        parquet_base = os.path.join(settings.SMB_PATH, "PB DB", "stock")
        write_stock_parquet(df_tok, parquet_base, bz=True)
    return df_tok


def run_get_last_three_months_including_current(typefile: str = "parquet") -> List[str]:
    """Generate rolling 3-month monthly stock data."""
    with execution_timer(f"get_last_3_months_stock_{typefile}"):
        today = datetime.now()
        last_dates = []

        for i in range(2, -1, -1):
            target_month = (today.month - i - 1) % 12 + 1
            target_year = today.year - ((today.month - i - 1) < 0)
            last_day = calendar.monthrange(target_year, target_month)[1]
            last_dates.append(f"{target_year}-{target_month:02d}-{last_day:02d}")

        if typefile == "parquet":
            for tgl in last_dates:
                logger.info(f"Generating monthly stock parquet for date: {tgl}")
                get_stock_monthly_parquet(tgl, typefile)
                get_stock_monthly_parquet_bz(tgl, typefile)
                merge_stock_by_date(tgl)

        gc.collect()
        return last_dates


def run_stock_and_sales(start_year: int = None, end_year: int = None, typefile: str = "parquet") -> None:
    """Merge monthly stock and sales data and persist to sales_thru_pb."""
    current_year = datetime.now().year
    if start_year is None:
        start_year = current_year
    if end_year is None:
        end_year = current_year

    with execution_timer(f"stock_and_sales_{start_year}_{end_year}_{typefile}"):
        from modules.proses import (
            get_stock_data_parquet, get_stock_data, merge_and_calculate,
            merge_detail, ensure_stock_columns
        )
        from modules.queries import get_monthly_sales_data

        base_path = os.path.join(settings.SMB_PATH, "PB DB")
        parquet_base = os.path.join(base_path, "stock")

        if typefile == 'parquet':
            stock_data = get_stock_data_parquet(parquet_base, start_year, end_year)
        else:
            stock_data = get_stock_data(base_path, start_year, end_year)

        sales_data = get_monthly_sales_data(start_year, end_year)
        final_data = merge_and_calculate(sales_data, stock_data, start_year, end_year)
        finale = merge_detail(final_data, stock_data, start_year, end_year)

        finale = finale.loc[:, ~finale.columns.duplicated()]
        finale = ensure_stock_columns(finale)
        finale = finale.drop(columns=[c for c in finale.columns if c.lower().startswith('ts_ ')], errors='ignore')

        nama_tabel = f'sales_thru_pb_{str(start_year)[-2:]}'
        safe_to_sql(finale, nama_tabel, stat='imp', if_exists='replace')

        add_index_and_foreign_keys(
            stat='imp',
            table_name=nama_tabel,
            index_cols=["fn_whconsiid", "fv_artsizecode"],
            foreign_keys=[
                {
                    "column": "fn_whconsiid",
                    "ref_table": "customers_pb",
                    "ref_column": "fn_whconsiid",
                    "constraint_name": f"fk_whconsiid_pb_sales_thru_{start_year}"
                },
                {
                    "column": "fv_artsizecode",
                    "ref_table": "artikel_pb",
                    "ref_column": "fv_artsizecode",
                    "constraint_name": f"fk_artsize_pb_sales_thru_{start_year}"
                }
            ]
        )
        gc.collect()


def run_fstok_gudang() -> None:
    """Process warehouse stock and export pivot table to Excel."""
    with execution_timer("fstok_gudang"):
        from modules.queries import df__stock_wh, df__fstock_wh, df_artikel_pb_, list_to_sql_in

        df_stock_wh = df__stock_wh()
        df_fstock_wh = df__fstock_wh()
        df_combi = pd.concat([df_stock_wh, df_fstock_wh], ignore_index=True)
        df_combi['fn_qty'] = df_combi['fn_qty'].astype(float)

        df_combi = df_combi.groupby(['fv_artsizecode', 'fv_whtypename'], as_index=False)['fn_qty'].sum()
        list_art = list_to_sql_in(df_combi, 'fv_artsizecode')
        df_artsize = df_artikel_pb_('fv_barcode,fn_articleid,fv_artsizecode,fv_sizename,fv_configname', distinct=True, where=f'and fv_artsizecode in ({list_art})')
        df_combi = df_combi.merge(df_artsize, how='left', on='fv_artsizecode')

        df_piv = pd.pivot_table(
            df_combi,
            values='fn_qty',
            index=['fv_barcode', 'fv_whtypename'],
            columns='fv_sizename',
            aggfunc='sum',
            fill_value=0
        ).reset_index()

        out_dir = os.path.join(settings.SMB_PATH, "For Alokator")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "Stok Gudang Berjalan.xlsx")
        df_piv.to_excel(out_path, index=False)
        logger.info(f"Saved warehouse stock report to: {out_path}")
        gc.collect()


def run_stock_by_artikel() -> None:
    """Consolidate stock aggregated by article across all stores."""
    with execution_timer("stock_by_artikel"):
        excel_path = os.path.join(settings.SMB_PATH, "for MD", "stock artikel all store 2026.xlsx")
        if not os.path.exists(excel_path):
            logger.warning(f"File not found: {excel_path}")
            return

        all_sheets = pd.read_excel(excel_path, sheet_name=None)
        df_excel = pd.concat(all_sheets.values(), ignore_index=True)
        df_excel = df_excel.drop(columns={'pk_id', 'size'}, errors='ignore')

        kolom_group = ['toko', 'code toko', 'artikel', 'bot/top', 'status']
        kolom_numerik = [col for col in df_excel.columns if col not in kolom_group]
        df_grouped = df_excel.groupby(kolom_group, as_index=False)[kolom_numerik].sum()

        out_dir = os.path.join(settings.SMB_PATH, "for Alokator")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "stock by artikel all store 2026.xlsx")
        df_grouped.to_excel(out_path, index=False)
        logger.info(f"Saved stock by artikel to: {out_path}")
        gc.collect()


def run_stock_borken_aging() -> None:
    """Process broken and aging warehouse stock calculations."""
    with execution_timer("stock_borken_aging"):
        from modules.queries import (
            df__draft_pending, df__customers_pb_, df__otsdo, df__otsdm, df__otsob,
            df__artikel_pb, df__stock_today_all_store, df__aging
        )

        df_pend = df__draft_pending(
            colom="a.fv_nodraft,fc_type,a.fc_status,b.fc_status,a.fn_whconsiid2,a.fn_whconsiid,fv_artsizecode,fn_on_book",
            a__fc_status="X", b__fc_status="X"
        )

        df_rt = df_pend.loc[df_pend['fc_type'] == "RT", ['fn_whconsiid', 'fv_artsizecode', 'fn_on_book']].rename(columns={"fn_on_book": "fn_qty"})
        df_mo = df_pend.loc[df_pend['fc_type'] == "MO", ['fn_whconsiid', 'fv_artsizecode', 'fn_on_book']].rename(columns={"fn_on_book": "fn_qty"})
        df_mi = df_pend.loc[df_pend['fc_type'] == "MO", ['fn_whconsiid2', 'fv_artsizecode', 'fn_on_book']].rename(columns={"fn_whconsiid2": "fn_whconsiid", "fn_on_book": "fn_qty"})
        df_ob = df_pend.loc[df_pend['fc_type'] == "OB", ['fn_whconsiid2', 'fv_artsizecode', 'fn_on_book']].rename(columns={"fn_whconsiid2": "fn_whconsiid", "fn_on_book": "fn_qty"})

        df_cus = df__customers_pb_("fn_cusid,fn_whconsiid")
        df_ostdo = df__otsdo()[['fn_whconsiid', 'fv_artsizecode', 'fn_qty']].dropna(subset='fn_whconsiid')
        df_ostdm = df__otsdm()[['fn_whconsiid2', 'fv_artsizecode', 'fn_qty']].rename(columns={"fn_whconsiid2": "fn_whconsiid"})
        df_ostob = df__otsob()[['fn_cusid', 'fv_artsizecode', 'OstOB']].rename(columns={"OstOB": "fn_qty"})
        df_ostob = df_ostob.merge(df_cus, how='left', on='fn_cusid')[['fn_whconsiid', 'fv_artsizecode', 'fn_qty']]

        df_rt['fn_qty'] = -df_rt['fn_qty']
        df_mo['fn_qty'] = -df_mo['fn_qty']

        df_combined = pd.concat([df_rt, df_mo, df_mi, df_ob, df_ostdo, df_ostdm, df_ostob], ignore_index=True)
        df_result = df_combined.groupby(['fn_whconsiid', 'fv_artsizecode'], as_index=False)['fn_qty'].sum()

        df_art = df__artikel_pb("fv_artsizecode,fv_barcode, fv_sizename, fv_configname")
        df_cus_toko = df__customers_pb_('fn_whconsiid, concat(fc_code," - ", fv_toko," - ",fv_kota) Toko', distinct=False, where='')

        df_export = df_art.merge(df_combined, how='right', on='fv_artsizecode')
        df_export = df_cus_toko.merge(df_export, how='right', on='fn_whconsiid')

        out_dir = os.path.join(settings.SMB_PATH, "For Alokator")
        os.makedirs(out_dir, exist_ok=True)
        df_export.to_excel(os.path.join(out_dir, "Detail Pendingan.xlsx"), index=False)

        logger.info(f"Successfully processed stock broken aging.")
        gc.collect()
