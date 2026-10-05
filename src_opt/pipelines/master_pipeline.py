"""
src_opt.pipelines.master_pipeline
Independent, high-performance master data synchronization pipeline.
Replaces column-by-column iteration with native MySQL bulk upserts and connection pooling.
Zero dependencies on legacy main.py.
"""
import os
import gc
from datetime import datetime
import numpy as np
import pandas as pd

from src_opt.config.settings import settings
from src_opt.config.database import get_engine, get_pooled_engine
from src_opt.config.logger import get_pipeline_logger
from src_opt.common.utils import execution_timer
from src_opt.common.db_helper import safe_to_sql, bulk_upsert

logger = get_pipeline_logger("src_opt.pipelines.master")


def run_upsert_artikel_pb() -> None:
    """
    Synchronize article master table (artikel_pb).
    Optimized with native MySQL bulk upsert (all columns in batch) instead of 22 separate column loops.
    """
    with execution_timer("upsert_artikel_pb"):
        from modules.queries import df__artikel_pb_read, df__artikel_status
        from modules.proses import get_artikel_md, add_ornal

        logger.info("Reading article master data from source DB...")
        df = df__artikel_pb_read()
        df = df.drop_duplicates(subset=['fv_artsizecode'])

        df_nfa = get_artikel_md()
        df = pd.merge(df, df_nfa, on='fv_barcode', how='left')
        df = df.loc[~df['fv_artsizecode'].isnull()]

        df[['fv_rating', 'fv_neo_link_foto']] = df[['fv_rating', 'fv_neo_link_foto']].fillna('none')
        df['fv_ornal'] = df.apply(add_ornal, axis=1)

        if df.empty:
            logger.warning("No article data found to upsert.")
            return

        df = df.fillna('none')
        df_art_status = df__artikel_status()
        df = pd.merge(df, df_art_status, on='fn_articleid', how='left')

        target_cols = [
            'fv_barcode', 'fv_artsizecode', 'fv_catname', 'fv_brandname', 'fv_divname',
            'fv_sizename', 'fm_price', 'fm_amount', 'fv_namemark', 'fv_colorname',
            'fv_fitname', 'fv_tomname', 'fv_configname', 'motif', 'fv_picture',
            'fv_rating', 'fm_cogs', 'fn_articleid', 'fv_neo_link_foto', 'fv_fitting',
            'fv_status', 'fv_ornal', 'fc_hold'
        ]
        available_cols = [c for c in target_cols if c in df.columns]
        df_clean = df[available_cols].copy()

        df_clean['fm_amount'] = pd.to_numeric(df_clean['fm_amount'], errors='coerce').fillna(0)
        df_clean['fm_price'] = pd.to_numeric(df_clean['fm_price'], errors='coerce').fillna(0)

        # High-performance bulk upsert: processes all columns simultaneously in chunks of 2,000 rows
        bulk_upsert(
            df=df_clean,
            table_name='artikel_pb',
            primary_key='fv_artsizecode',
            stat='imp',
            chunksize=2000
        )
        gc.collect()


def run_upsert_customers_pb() -> None:
    """
    Synchronize customer master table (customers_pb).
    Integrates external spreadsheet data and updates records via bulk upsert.
    """
    with execution_timer("upsert_customers_pb"):
        from modules.queries import df__customers_pb_read, df__cus_user
        from modules.proses import process_data_cus

        base_path = settings.SMB_PATH + "/PB DB/" if not settings.SMB_PATH.endswith("/") else settings.SMB_PATH + "PB DB/"
        base_path2 = settings.SMB_PATH + "/For Alokator/" if not settings.SMB_PATH.endswith("/") else settings.SMB_PATH + "For Alokator/"

        excel_path = os.path.join(base_path, "customers_pb_sup.xlsx")
        csv_path = os.path.join(base_path, "list user & employee.csv")
        pic_path = os.path.join(base_path, "LIST PIC MARKETING.xlsx")
        mic_path = os.path.join(base_path, "PIC MIC.xlsx")
        mic_path2 = os.path.join(base_path2, "data toko.xlsx")

        logger.info("Reading customers base from read DB...")
        df = df__customers_pb_read()

        if os.path.exists(excel_path):
            df_excel = pd.read_excel(excel_path)
        else:
            logger.warning(f"File not found: {excel_path}, skipping merge")
            df_excel = pd.DataFrame()

        if os.path.exists(csv_path):
            emp_tb = pd.read_csv(csv_path)
            emp_tb['fn_userid'] = emp_tb['fn_userid'].astype(int)
            df_user = df__cus_user()
            df_user = pd.merge(df_user, emp_tb[['fn_userid', 'fv_nameemployees']], on='fn_userid', how='left')
            df_user['fn_cusid'] = df_user['fn_cusid'].astype(int)
            df = pd.merge(df, df_user[['fn_cusid', 'fv_nameemployees']], on='fn_cusid', how='left')

        df = df.loc[~df['fn_whconsiid'].duplicated()]
        if not df_excel.empty:
            df = pd.merge(df, df_excel, on='fn_cusid', how='left')

        if os.path.exists(pic_path):
            df_picmkt = pd.read_excel(pic_path, header=3).dropna(how='all')
            if 'SUPERVISOR/ OJT**' in df_picmkt.columns:
                df_picmkt['SUPERVISOR/ OJT**'] = df_picmkt['SUPERVISOR/ OJT**'].astype(str).str.replace(r'[^A-Za-z\s]', '', regex=True).str.strip().str.title()
                df = pd.merge(df, df_picmkt[['MANAGER', 'SUPERVISOR/ OJT**', 'KODE TOKO GIS']], left_on='code', right_on='KODE TOKO GIS', how='left')
                df = df.rename(columns={'MANAGER_y': 'MANAGER'})
                df['SUPERVISOR'] = df['SUPERVISOR/ OJT**']

        if os.path.exists(mic_path):
            df_picmic = pd.read_excel(mic_path).dropna(how='all')
            if 'STORE GIS' in df_picmic.columns:
                df = pd.merge(df, df_picmic[['PIC MIC', 'STORE GIS', 'SPG Konsinetto']], left_on='code', right_on='STORE GIS', how='left').fillna('-')

        if os.path.exists(mic_path2):
            df_picmic2 = pd.read_excel(mic_path2).dropna(how='all')
            if 'kode_toko' in df_picmic2.columns:
                df = pd.merge(df, df_picmic2[['kode_toko', 'pic']], left_on='code', right_on='kode_toko', how='left').fillna('-')
                df['Alokator'] = df['pic']

        df.loc[df['fn_cusid'] == 915, 'fn_whconsiid'] = 9000
        drop_cols = [c for c in ['KODE TOKO GIS', 'MANAGER_x', 'SUPERVISOR/ OJT**', 'STORE GIS', 'PIC MIC', 'pic', 'kode_toko'] if c in df.columns]
        df = df.drop(columns=drop_cols)

        if 'fn_grpcusid' in df.columns and 'Neo Toko' in df.columns:
            df.loc[df['fn_grpcusid'] == 9, 'Neo Toko'] = "Bazar"
        if 'Toko' in df.columns and 'fc_codecus' in df.columns:
            df.loc[df['Toko'].astype(str).str.startswith('RJ-FO'), 'fc_codecus'] = df['code']
            df.loc[df['fc_codecus'].astype(str).str.startswith('-'), 'fc_codecus'] = df['code']
        if 'SPG Konsinetto' in df.columns:
            df = df.rename(columns={'SPG Konsinetto': 'fv_spg_kosninetto'})

        df = process_data_cus(df)
        df = df.where(pd.notnull(df), None)

        bulk_upsert(df, table_name='customers_pb', primary_key='fn_whconsiid', stat='imp', chunksize=2000)
        gc.collect()


def run_upsert_tgl_so_sa() -> None:
    """Synchronize stock opname and stock initial dates (tgl_so_sa)."""
    with execution_timer("upsert_tgl_so_sa"):
        from modules.queries import df__tglsa_, df__tglso_
        logger.info("Synchronizing tgl_sa and tgl_so...")
        df_sa = df__tglsa_()
        safe_to_sql(df_sa, 'tgl_sa', stat='imp', if_exists='replace')

        df_so = df__tglso_()
        safe_to_sql(df_so, 'tgl_so', stat='imp', if_exists='replace')
        gc.collect()


def run_upsert_whtype() -> None:
    """Synchronize warehouse type definitions."""
    with execution_timer("upsert_whtype"):
        from modules.queries import df__whtype_name
        logger.info("Synchronizing whtype definitions...")
        df = df__whtype_name()
        safe_to_sql(df, 'whtype', stat='imp', if_exists='replace')
        gc.collect()


def run_upsert_master() -> None:
    """Consolidate master article data with warehouse aging into Excel."""
    with execution_timer("upsert_master"):
        from modules.queries import df__artikel_pb, df__aging_gudang_pb
        logger.info("Generating master article excel report...")
        df2 = df__artikel_pb("*")
        df_aging = df__aging_gudang_pb()
        df2 = df2.merge(df_aging, how='left', on='fn_articleid').rename(columns={'fn_hari_fdo': 'aging'})

        out_dir = os.path.join(settings.SMB_PATH, "For Alokator")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "MASTER ARTIKEL.xlsx")
        df2.to_excel(out_path, index=False)
        logger.info(f"Saved master artikel to {out_path}")
        gc.collect()


def run_aging_gudang_pb() -> None:
    """Compute and update warehouse aging data into aging_gudang_pb."""
    with execution_timer("aging_gudang_pb"):
        logger.info("Computing warehouse arrival aging...")
        query_gr = '''
        SELECT a.fc_nodoc, DATE(fd_arrivaldate) AS fd_arrivaldate, fv_nopo, fn_articleid, fn_qty 
        FROM grdtl_tb a
        INNER JOIN grmst_tb b ON a.fc_nodoc = b.fc_nodoc
        UNION ALL 
        SELECT a.fc_nodoc, DATE(a.fd_dateinput) AS fd_arrivaldate, a.fc_nodoc AS fv_nopo, fn_articleid, fn_qty
        FROM stockindtl_tb a 
        INNER JOIN stockinmst_tb b ON a.fc_nodoc = b.fc_nodoc
        '''
        with get_engine('write') as engine_write:
            df_gr = pd.read_sql(query_gr, engine_write)

        result = df_gr.sort_values('fn_qty', ascending=False).groupby(['fv_nopo', 'fn_articleid']).first().reset_index()
        df_sorted = result.sort_values('fd_arrivaldate', ascending=False)
        df_latest = df_sorted.drop_duplicates(subset='fn_articleid', keep='first')

        query_art = "SELECT DISTINCT fn_articleid, fv_barcode FROM gis_local.artikel_pb"
        with get_engine('imp') as engine_imp:
            df_art = pd.read_sql(query_art, engine_imp)

        df_latest = pd.merge(df_latest, df_art, on='fn_articleid', how='right')
        df_latest = df_latest.fillna({'fn_qty': 0, 'fd_arrivaldate': '2023-01-01', 'fv_nopo': '', 'fc_nodoc': ''})
        df_latest["fd_arrivaldate"] = pd.to_datetime(df_latest["fd_arrivaldate"])

        today = pd.Timestamp(datetime.now().date())
        df_latest["fn_hari"] = (today - df_latest["fd_arrivaldate"]).dt.days
        df_latest["fn_bulan"] = (today.year - df_latest["fd_arrivaldate"].dt.year) * 12 + (today.month - df_latest["fd_arrivaldate"].dt.month)

        query_fdo = '''
        SELECT DISTINCT fn_articleid, a.fd_dateinput 
        FROM dodtl_tb a
        LEFT JOIN domst_tb b ON a.fc_nodoc = b.fc_nodoc 
        WHERE a.fc_nodoc LIKE "DO%%"
        '''
        with get_engine('write') as engine_write:
            df_fdo = pd.read_sql(query_fdo, engine_write)

        df_unique = df_fdo.sort_values('fd_dateinput').drop_duplicates(subset='fn_articleid', keep='first')
        df_latest = pd.merge(df_latest, df_unique, on='fn_articleid', how='left')
        df_latest["fn_hari_fdo"] = (today - df_latest["fd_dateinput"]).dt.days
        df_latest["fn_bulan_fdo"] = (today.year - df_latest["fd_dateinput"].dt.year) * 12 + (today.month - df_latest["fd_dateinput"].dt.month)

        df_latest = df_latest.assign(
            fd_dateinput=df_latest['fd_dateinput'].combine_first(df_latest['fd_arrivaldate']),
            fn_hari_fdo=df_latest['fn_hari_fdo'].combine_first(df_latest['fn_hari']),
            fn_bulan_fdo=df_latest['fn_bulan_fdo'].combine_first(df_latest['fn_bulan'])
        )
        safe_to_sql(df_latest, 'aging_gudang_pb', stat='imp', if_exists='replace')
        gc.collect()


def run_upsert_expedition_tracking(year: int = None) -> None:
    """Synchronize expedition tracking and delivery status."""
    if year is None:
        year = datetime.now().year

    with execution_timer("upsert_expedition_tracking"):
        from modules.proses import get_nodo_tglreceive
        from modules.queries import df__date_arrival, df__tracking_do, df__tracking_rm, df__detail_tracking

        logger.info(f"Processing expedition tracking for year {year}...")
        get_nodo_tglreceive('yes', year)

        df_date_arv = df__date_arrival()
        df_do = df__tracking_do()
        df_rm = df__tracking_rm()

        df = pd.concat([df_do, df_rm], ignore_index=True)
        df = df.merge(df_date_arv, left_on='nodo', right_on='No. DO', how='left')
        df = df.rename(columns={'Date of Receipt': 'date_arrival', 'Description': 'nosi', 'No. Tracking Number': 'nosi_add'})
        df['fn_cusid'] = df['fn_cusid'].fillna(0).astype(int)
        df['nodo'] = df['nodo'].fillna(df['noob'])
        df = df.rename(columns={'nosi': 'fv_nosi', 'fv_nosi': 'fv_nosi_sistem'})
        df['fn_whconsiid2'] = df['fn_whconsiid2'].fillna(0).astype(int)

        conditions = [
            df['nodo'].fillna('').str.startswith('DO'),
            df['nodo'].fillna('').str.startswith('DM'),
            df['nodo'].isna()
        ]
        choices = ['DO', 'DM', 'RT']
        df['fc_ket_sj'] = np.select(conditions, choices, default='RT')

        safe_to_sql(df, 'expedition_tracking_pb', stat='imp', if_exists='replace')

        df_detail = df__detail_tracking(year)
        table_dtl = f'expedition_tracking_dtl_pb_{year}'
        safe_to_sql(df_detail, table_dtl, stat='imp', if_exists='replace')
        gc.collect()
