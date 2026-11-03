from modules.queries import (df__tglso, df__tglsa, df__disc2, df__margin, df__artsizecode, df__stock_so,
                             df__stock_sa, df__retur, df__size, generate_months_for_year, df__rtt,df__penju_tabel,
                             df__sls_putus_disc, df__aging, df__customers_pb,df__date_arrival,list_to_sql_in,
                             df_artikel_pb_, df__domst_tb,df__customers_pb_,df__otsdmdo_rt, df__ots_retur_gd)
from modules.db_connection import get_engine
from sqlalchemy import Integer, String, CHAR, DateTime, Text, text, Float, Numeric
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import MetaData
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import sessionmaker

import openpyxl
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
import traceback
import os
import re

def clean_and_transform_data(df):
    """Melakukan pembersihan dan transformasi data."""
    df['tgltrx'] = pd.to_datetime(df['tgltrx'], format='%m-%d-%Y')
    df['bulan'] = df['tgltrx'].dt.month
    df['nama_bulan'] = df['tgltrx'].dt.strftime('%B')
    df['tahun'] = df['tgltrx'].dt.year

    df['fv_artsizecode'] = df['fv_artsizecode'].str.strip().str.upper()
    df['status_artikel'] = df['status_artikel'].fillna(df['event'])

    df['fv_artsizecode'] = df['fv_artsizecode'].replace({'BBRD103V042333-': 'BBRD103V042333'})
    return df

def insert_disc2_grouping(df,type):
    df_disc2= df__disc2()
    df = pd.merge(df, df_disc2, on='fv_nopenjualan', how='left')
    df = df.loc[~df.duplicated()]
    df[['artsize_disc2nd','artsize_servisfee','artsize_premi',
            'artsize_affiliate','artsize_voucher']] = df[['artsize_disc2nd','artsize_servisfee','artsize_premi',
            'artsize_affiliate','artsize_voucher']].fillna(0)
    df['total_disc2']=df['artsize_disc2nd']*df['fn_jualpersize']
    sum_col=['fn_jualpersize','total_penjualan2','total_disc2',
                'artsize_servisfee','artsize_premi','artsize_affiliate','artsize_voucher']
    agg_col=['fv_nopenjualan','fn_whconsiid', 'fv_artsizecode', 'tgltrx']
    fisrt_col=[col for col in df.columns if col not in sum_col + agg_col]
    print(f" {sum_col}")
    print(f" {fisrt_col}")
    if type == 'sum':
        df = df.groupby(['fv_nopenjualan','fn_whconsiid', 'fv_artsizecode', 'tgltrx']).agg({
            **{col: 'sum' for col in sum_col} , **{col: 'first' for col in fisrt_col}
            }).reset_index()
    else:
        return df
    return df

def insert_margin(df,year):
    df_margin= df__margin(year)
    df = pd.merge(df, df_margin[['fn_idurut','fn_margin','fc_status']], on='fn_idurut', how='left')
    df = df.loc[~df['fn_idurut'].duplicated()]
    return df

def rename_col_py_to_sql(df):
    # Buat dictionary untuk rename kolom
    name_col = {
        'total_qty': 'fn_totalqty',
        'total_penjualan': 'fn_hargajual',
        'hargajual': 'fn_hargajual',
        'total_penjualan2': 'fn_totalpenjualan',
        'total_disc2':'fn_total_disc2',
        'tgltrx': 'fd_tgltrx',
        'disc_id': 'fc_discid',
        'status_artikel': 'fv_status_artikel',
        'desc': 'fv_desc',
        'event': 'fv_event',
        'bulan': 'fn_month',
        'nama_bulan': 'fc_monthname',
        'tahun': 'fn_year',
        'voucher': 'fv_voucher',
        'artsize_disc2nd':'fn_artsize_disc2nd',
        'disc2nd':'fn_disc2nd',
        'Toko': 'fv_toko',
        'Provinsi': 'fv_provinsi',
        'Kota': 'fv_kota',
        'cusid': 'fn_cusid',
        'code': 'fc_code',
        'Neo Toko': 'fv_neotoko',
        'MANAGER': 'fv_manager',
        'CHIEF SUPERVISOR': 'fv_chief_supervisor',
        'SUPERVISOR': 'fv_spv',
        'Alokator': 'fv_alokator',
        'WADIR': 'fv_wadir',
        'KOREM': 'fv_korem',
        'MMD': 'fv_mmd',
        'CSE/IC': 'fv_cse_ic',
        'dpp':'fn_dpp',
        'disctok':'fn_disctok',
        'fin_dpp':'fn_fin_dpp',
        'total_amount':'fn_total_amount',
        'fc_nodoc':'fv_nodoc',
    }
    # Lakukan rename dan sort
    df = df.rename(columns=name_col)
    return df

def rename_col_sql_to_xlsx_gis(df):
    # Buat dictionary untuk rename kolom
    name_col = {
        'fd_tgltrx': 'Sales Date',
        'fc_code': 'Code Store',
        'fv_toko': 'Store',
        'fv_barcode':'Code Articel',
        'fv_catname': 'Kategori',
        'fv_fitname': 'Name Articel',
        'fv_brandname': 'Brand',
        'fv_divname': 'Division',
        'fv_sizename': 'Size',
        'fv_event': 'Status Article',
        'fm_price': 'HET',
        'fn_totalpenjualan': 'Total Nett Kassa',
        'fn_nett_amount': 'Nett Amount',
        'fn_jualpersize':'Qty',
        'fv_ornal': 'Ornal',
        'fv_status':'Status Produksi',
        'fv_colorname':'Color',
        'fv_fitting':'Fitting',
        'motif':'Motif',
        'fn_total_disc2':"Disc Nota"
        # 'fv_namemark':'Status Master'

    }
    # Lakukan rename dan sort
    df = df.rename(columns=name_col)
    return df

def insert_pk(df):
    df_artsizecode = df__artsizecode(stat='write')
    df=pd.merge(df,df_artsizecode,on='fv_artsizecode',how='left')
    df["fd_tgltrx"] = pd.to_datetime(df["fd_tgltrx"], format="%d/%m/%Y").dt.strftime('%y%m%d')
    df["pk_id"] = df["fn_whconsiid"].astype(str) + "/" + df["fd_tgltrx"] + "/" + df["fn_artsizeid"].astype(str)
    # Menyisipkan kolom 'pk_id' di posisi pertama (index 0)
    df.insert(0, "pk_id", df.pop("pk_id"))
    return df



def define_sql_types(df):
    """Menentukan tipe data untuk SQL sesuai dengan tipe kolom."""
    sql_types = {}
    for col in df.columns:
        col_data = df[col]
        if isinstance(col_data, pd.DataFrame):
            print(f"⚠️ Kolom {col} duplikat, ambil kolom pertama")
            col_data = col_data.iloc[:, 0]

        col_dtype = col_data.dtype

        # Jika kolom tipe float
        if col_dtype == 'float64':
            sql_types[col] = Numeric(15, 2)
        # Jika kolom diawali 'fn' dan bukan float
        elif col.startswith('fn'):
            sql_types[col] = Integer
        # Jika kolom diawali 'fv'
        elif col.startswith('fv'):
            max_length = df[col].astype(str).map(len).max()
            if max_length > 255:
                sql_types[col] = Text
            else:
                sql_types[col] = String(255)
        # Jika kolom diawali 'fc'
        elif col.startswith('fc'):
            sql_types[col] = CHAR(20)
        # Jika kolom diawali 'fd'
        elif col.startswith('fd'):
            sql_types[col] = DateTime
        # Kalau tidak masuk kriteria di atas
        else:
            sql_types[col] = String(50)
    
    return sql_types


def save_to_sql(df, table_name, engine):
    """Menyimpan data ke dalam database menggunakan SQLAlchemy."""
    with get_engine(engine) as engine:
        sql_types = define_sql_types(df)
        df.to_sql(table_name, con=engine, if_exists='replace', index=False, 
        chunksize=5000,   # 🔥 penting
        method='multi', 
        dtype=sql_types)

def add_index(engine, table_name, index_cols):
    """
    Menambahkan composite index ke tabel SQL dengan error handling.

    Parameters:
    - engine: objek engine SQLAlchemy
    - table_name: nama tabel
    - index_cols: list kolom untuk composite index
    """
    if not index_cols:
        print("⚠️ Tidak ada kolom yang diberikan untuk membuat index.")
        return

    index_name = f"idx_{'_'.join(index_cols)}"
    cols_joined = ', '.join(index_cols)

    with get_engine(engine) as engine:
        with engine.connect() as conn:
            try:
                conn.execute(text(f"CREATE INDEX {index_name} ON {table_name} ({cols_joined})"))
                print(f"✅ Index '{index_name}' berhasil ditambahkan.")
            except Exception as e:
                print(f"⚠️ Gagal membuat index '{index_name}': {e}")

def add_primary_key(engine, table_name, column_name):
    """
    Menambahkan PRIMARY KEY ke kolom dalam sebuah tabel.

    Parameters:
    - engine: SQLAlchemy engine (connection ke database)
    - table_name: nama tabel target (string)
    - column_name: nama kolom yang ingin dijadikan primary key (string)
    """
    with get_engine(engine) as engine:
        with engine.connect() as conn:
            try:
                alter_stmt = text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({column_name})")
                conn.execute(alter_stmt)
                print(f"Primary key berhasil ditambahkan pada {table_name}.{column_name}")
            except Exception as e:
                print(f"Gagal menambahkan primary key: {e}")

def add_foreign_keys(engine, table_name, foreign_keys):
    """
    Menambahkan foreign keys ke tabel SQL dengan error handling.

    Parameters:
    - engine: objek engine SQLAlchemy
    - table_name: nama tabel
    - foreign_keys: list of dicts dengan kunci:
        {
            "column": "kolom_di_tabel_ini",
            "ref_table": "tabel_referensi",
            "ref_column": "kolom_di_tabel_referensi",
            "constraint_name": "nama_konstraint"
        }
    """
    if not foreign_keys:
        print("⚠️ Tidak ada foreign key yang diberikan.")
        return

    with get_engine(engine) as engine:
        with engine.connect() as conn:
            for fk in foreign_keys:
                column = fk["column"]
                ref_table = fk["ref_table"]
                ref_column = fk["ref_column"]
                constraint_name = fk["constraint_name"]

                fk_sql = f"""
                    ALTER TABLE {table_name}
                    ADD CONSTRAINT {constraint_name}
                    FOREIGN KEY ({column})
                    REFERENCES {ref_table} ({ref_column})
                """
                try:
                    conn.execute(text(fk_sql))
                    print(f"✅ Foreign key '{constraint_name}' berhasil ditambahkan.")
                except Exception as e:
                    print(f"⚠️ Gagal menambahkan foreign key '{constraint_name}': {e}")

def add_index_and_foreign_keys(engine, table_name, index_cols=[], foreign_keys=[]):
    """
    Menambahkan composite index dan foreign keys ke tabel SQL dengan error handling.

    Parameters:
    - conn: objek koneksi SQLAlchemy
    - table_name: nama tabel yang akan dimodifikasi
    - index_cols: list kolom untuk composite index
    - foreign_keys: list of dicts:
        {
            "column": "kolom_di_tabel_ini",
            "ref_table": "tabel_referensi",
            "ref_column": "kolom_di_tabel_referensi",
            "constraint_name": "nama_konstraint"
        }
    """
    with get_engine(engine) as engine:
        with engine.connect() as conn:
        # Tambahkan Composite Index
            if index_cols:
                index_name = f"idx_{'_'.join(index_cols)}"
                cols_joined = ', '.join(index_cols)
                try:
                    conn.execute(text(f"CREATE INDEX {index_name} ON {table_name} ({cols_joined})"))
                    print(f"✅ Index '{index_name}' berhasil ditambahkan.")
                except Exception as e:
                    print(f"⚠️ Gagal membuat index '{index_name}': {e}")

            # Tambahkan Foreign Keys
            for fk in foreign_keys:
                column = fk["column"]
                ref_table = fk["ref_table"]
                ref_column = fk["ref_column"]
                constraint_name = fk["constraint_name"]

                fk_sql = f"""
                    ALTER TABLE {table_name}
                    ADD CONSTRAINT {constraint_name}
                    FOREIGN KEY ({column})
                    REFERENCES {ref_table} ({ref_column})
                """
                try:
                    conn.execute(text(fk_sql))
                    print(f"✅ Foreign key '{constraint_name}' berhasil ditambahkan.")
                except Exception as e:
                    print(f"⚠️ Gagal menambahkan foreign key '{constraint_name}': {e}")

def get_tgl(idwhtoko, tglnow, tglEND):
    df_tglawal= df__tglso(idwhtoko, tglnow)
    if df_tglawal.empty:
            # Jika data kosong, ambil dari saldoawalarticleconsi_tb
            df_tglawal = df__tglsa(idwhtoko, tglnow)

        # Cek kondisi: Jika toko memiliki hold 'T' dan tanggal kurang dari tglEND, hentikan
    if not df_tglawal.empty:
        if (df_tglawal['fd_tglflagIndo'].iloc[0] < tglEND) and (df_tglawal['fc_hold'].iloc[0] == 'T'):
            return None
    df_tglawal = df__tglso(idwhtoko, tglEND)
        
    if df_tglawal.empty:
        df_tglawal = df__tglsa(idwhtoko, tglEND)
    return df_tglawal

def generate_stock_files(base_path, start_year, end_year):
    stock_files = []
    months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    
    for year in range(start_year, end_year + 1):
        for month in months:
            file_path = os.path.join(base_path, f'pb_stock_{month}_{str(year)[-2:]}.xlsx')
            stock_files.append(file_path)
            
            # Jika bulan Desember, tambahkan file untuk Januari tahun berikutnya
            if month == 'dec' :  # Pastikan tidak melebihi end_year
                next_year = year + 1
                january_file_path = os.path.join(base_path, f'pb_stock_jan_{str(next_year)[-2:]}.xlsx')
                stock_files.append(january_file_path)
    return stock_files

def get_stock(idwhtoko, tglSTR, tglEND):
    df_stock = df__stock_so(idwhtoko, tglSTR, tglEND)
    if df_stock['qty'].sum() == 0:
        df_stock = df__stock_sa(idwhtoko, tglEND)
    return df_stock

import pandas as pd
import os

def get_stock_data(base_path, start_year, end_year):
    base_path = convert_to_local_path(base_path)
    stock_files = generate_stock_files(base_path, start_year, end_year)
    print("Daftar file yang dicek:", stock_files)
    
    all_data = []

    for file in stock_files:
        if os.path.exists(file):
            print(f"✅ Membaca: {file}")
            
            # Deteksi jumlah sheet
            excel_sheets = pd.ExcelFile(file)
            sheet_names = excel_sheets.sheet_names

            if len(sheet_names) == 1:
                df = pd.read_excel(file)
            else:
                # Gabungkan semua sheet
                df_dict = pd.read_excel(file, sheet_name=None)
                df = pd.concat(df_dict.values(), ignore_index=True)

            all_data.append(df)

        else:
            print(f"❌ File tidak ditemukan: {file}")

    # Gabung semua data dari file
    combined_data = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    print("Sebelum groupby:", combined_data.shape)

    # Group jika kolom lengkap
    if not combined_data.empty and 'fn_whconsiid' in combined_data.columns and 'fv_artsizecode' in combined_data.columns:
        combined_data = combined_data.groupby(['fn_whconsiid', 'fv_artsizecode'], as_index=False).sum()
        print("Setelah groupby:", combined_data.shape)
    else:
        print("⚠️ Data kosong atau kolom tidak lengkap.")
    
    return combined_data


def test_get_stock_data():
    base_path = '/opt/share/PB DB/pb_stock_may_25.xlsx'
    print("Base path:", base_path)
    print(os.path.exists(base_path))
    # stock_files = generate_stock_files(base_path, start_year, end_year)
    combined_data=pd.read_excel(base_path)
    print(combined_data.info())
    return combined_data

def merge_and_calculate(sales_data, stock_data, start_year, end_year):
    # Menggabungkan data penjualan dan stok
    sales_data['fv_artsizecode'] = sales_data['fv_artsizecode'].astype(str).str.strip().str.upper()
    stock_data['fv_artsizecode'] = stock_data['fv_artsizecode'].astype(str).str.strip().str.upper()

    combined_data = pd.merge(stock_data, sales_data, on=['fn_whconsiid', 'fv_artsizecode'], how='outer').fillna(0)
    # combined_data=combined_data.drop(columns='tgltrx')
    # Generate nama kolom stok dan qty berdasarkan tahun
    periods = generate_months_for_year(start_year, end_year)  # Gunakan function yang baru dibuat
    months = sorted(
            periods.keys(),
            key=lambda x: datetime.strptime(x, '%b_%y')
        )


    # Hitung total stok per bulan
    for i, month in enumerate(months):
        next_month_stock = f'stock {months[i+1]}' if i < len(months) - 1 else None
        qty_col = f'qty_{month}'
        combined_data[f'total_stock_{month}'] = combined_data.get(next_month_stock, 0) + combined_data.get(qty_col, 0)

    # Hitung total kuantitas dan total stok
    qty_cols = [f'qty_{m}' for m in months]
    stock_cols= [f'stock {m}' for m in months]
    # Cari kolom stok yang ada dalam data
    for col in stock_cols:
        if col not in combined_data.columns:
            combined_data[col] = 0
    available_stock_cols = [col for col in stock_data.columns if col.startswith('stock ')]

    # Pilih kolom stok terakhir (sesuai urutan bulan)
    if available_stock_cols:
        def extract_month(col):
            raw = col.replace('stock ', '').strip()
            # Dukung format stok akhir tahun 'at_xx' atau 'at xx' (misal: 'at_26' -> 31 Des)
            if raw.startswith('at_') or raw.startswith('at '):
                year_str = raw[3:].strip()
                try:
                    year = int('20' + year_str if len(year_str) == 2 else year_str)
                    return datetime(year, 12, 31, 23, 59, 59)
                except ValueError:
                    return datetime.max
            try:
                return datetime.strptime(raw, '%b_%y')
            except ValueError:
                try:
                    return datetime.strptime(raw, '%b %y')
                except ValueError:
                    return datetime.min

        last_stock_col = max(
            available_stock_cols,
            key=extract_month
        )
    else:
        last_stock_col = None  # Jika tidak ada stok, gunakan None
    # Pastikan semua kolom qty ada (isi 0 jika belum ada)
    for col in qty_cols:
        if col not in combined_data.columns:
            combined_data[col] = 0
    combined_data['total_qty'] = combined_data[qty_cols].sum(axis=1)
    # Perhitungan total_stock
    if last_stock_col:
        combined_data['total_stock'] = combined_data[last_stock_col] + combined_data['total_qty']
    else:
        combined_data['total_stock'] = combined_data['total_qty']

    # Membersihkan data kolom 'fv_artsizecode'

    combined_data['fv_artsizecode'] = combined_data['fv_artsizecode'].str.upper().str.replace(r'BBRD103V042333-', 'BBRD103V042333')

    # Ambil data retur
    df_retur = df__retur(start_year)
    # Mengubah nilai qty menjadi negatif
    df_retur['qty'] = pd.to_numeric(df_retur['qty'], errors='coerce') * -1

    # Merge data retur dengan data utama
    combined_data = pd.merge(combined_data, df_retur, on=['fn_whconsiid', 'fv_artsizecode'], how='left')
    combined_data['qty'] = combined_data['qty'].fillna(0)

    # Bersihkan data (hapus baris jika semua kolom = 0)
    combined_data['min'] = combined_data[stock_cols + ['total_stock']+['qty']].min(axis=1)
    combined_data['max'] = combined_data[stock_cols + ['total_stock']+['qty']].max(axis=1)
    combined_data = combined_data[(combined_data['min'] != 0) | (combined_data['max'] != 0)]
    return combined_data


def merge_detail(final_data, stock_data, start_year, end_year):
    df_articlesize = df__artsizecode(stat='write')
    df_size = df__size()

    final_data = pd.merge(final_data, df_articlesize, on='fv_artsizecode', how='left')
    final_data = pd.merge(final_data, df_size, on='fn_sizeid', how='left')
    final_data = final_data[~final_data['fv_sizename'].isnull()]

    # Membuat kolom secara dinamis untuk semua bulan di rentang tahun
    columns = ['fn_artsizeid', 'fn_whconsiid', 'fv_artsizecode']
    rename_columns = {'qty': 'fn_qty_retur'}
    month_names = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

    current_year = datetime.now().year
    current_month = min(datetime.now().month + 1, 12)
    # last_stock_col = None  # Variabel untuk menyimpan kolom stock terakhir

    for year in range(start_year, end_year + 1):
        last_month = current_month if year == current_year else 12  # Sampai bulan saat ini di tahun berjalan
        stock_at_col = f"stock at_{str(year)[-2:]}"
        has_stock_at = stock_at_col in stock_data.columns

        for month in range(1, last_month + 1):
            month_str = f"{month_names[month-1]}_{str(year)[-2:]}"  # Format jan_25, feb_25, dst.
            col_prefix = f'{month:02d}'
            
            stock_col = f'stock {month_str}'

            if has_stock_at:
                # Jika ada stock_at (bulan 13), seluruh bulan 1 s.d. last_month adalah kolom bulanan
                total_stock_col = f'total_stock_{month_str}'
                qty_col = f'qty_{month_str}'
                
                columns.extend([stock_col, total_stock_col, qty_col])
                rename_columns.update({
                    qty_col: f'fn_qty_{col_prefix}',
                    stock_col: f'fn_stock_{col_prefix}',
                    total_stock_col: f'fn_ts_{col_prefix}'
                })
            else:
                # Hanya menyimpan stock bulan terakhir jika tidak ada stock_at
                if month == last_month:
                    columns.append(stock_col)
                    rename_columns[stock_col] = 'fn_stock_at'
                else:
                    total_stock_col = f'total_stock_{month_str}'
                    qty_col = f'qty_{month_str}'
                    
                    columns.extend([stock_col, total_stock_col, qty_col])
                    rename_columns.update({
                        qty_col: f'fn_qty_{col_prefix}',
                        stock_col: f'fn_stock_{col_prefix}',
                        total_stock_col: f'fn_ts_{col_prefix}'
                    })
            
            if month == 12:
                available_stock_cols = [col for col in stock_data.columns if col.startswith('stock ')]

                # Pilih kolom stok terakhir (sesuai urutan bulan)
                if available_stock_cols:
                    last_stock_col = (available_stock_cols)[-1]  # Ambil stok terakhir berdasarkan nama kolom
                else:
                    last_stock_col = None  # Jika tidak ada stok, gunakan None
                
                total_stock_dec = f'total_stock_dec_{str(year)[-2:]}'
                qty_dec = f'qty_dec_{str(year)[-2:]}'
                stock_dec = f'stock dec_{str(year)[-2:]}'
                columns.extend([total_stock_dec, qty_dec, last_stock_col])
                rename_columns.update({
                    stock_dec: 'fn_stock_12',
                    total_stock_dec: 'fn_ts_12',
                    qty_dec: 'fn_qty_12',
                    last_stock_col: 'fn_stock_at'
                })

        if has_stock_at and stock_at_col not in columns:
            columns.append(stock_at_col)
            rename_columns[stock_at_col] = 'fn_stock_at'
    # additional condition
    columns.extend([])
    columns.extend(['qty', 'total_stock', 'total_qty'])
    rename_columns.update({
        'total_qty': 'fn_total_sales_qty',
        'total_stock': 'fn_ts_all'
    })
    final_data = final_data[columns]
    final_data = final_data.rename(columns=rename_columns).sort_values(by='fn_whconsiid')
    final_data = final_data.loc[:, ~final_data.columns.duplicated()]
    if month == 12:
        final_data['fn_ts_12']=final_data['fn_ts_12']+final_data['fn_stock_at']
    return final_data

def ensure_stock_columns(df, base_col='fn_stock', fallback_col='fn_stock_at', start=1, end=12):
    """
    Memastikan kolom fn_stock_01 s.d fn_stock_12 tersedia.
    Jika tidak ada, kolom akan dibuat dan diisi dari fallback_col.
    """

    if fallback_col not in df.columns:
        raise ValueError(f"Kolom fallback '{fallback_col}' tidak ditemukan di DataFrame")

    for i in range(start, end + 1):
        col_name = f"{base_col}_{i:02d}"
        if col_name not in df.columns:
            df[col_name] = df[fallback_col]

    return df

def generate_rename_dict(start_year, end_year, df):
    # Buat dictionary untuk bulan
    months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    # Dictionary comprehension untuk kolom qty, stock, dan total stock
    month_columns = {}
    for year in range(start_year, end_year + 1):
            for i in range(1, 13):
                month_columns[f'fn_qty_{str(i).zfill(2)}'] = f'qty {months[i-1]} {str(year)[-2:]}'
                month_columns[f'fn_stock_{str(i).zfill(2)}'] = f'stock {months[i-1]} {str(year)[-2:]}'
                month_columns[f'fn_ts_{str(i).zfill(2)}'] = f'total stock {months[i-1]} {str(year)[-2:]}'

    # Dictionary tambahan untuk kolom di luar bulan
    other_columns = {
        'fn_qty_retur': f'qty retur {str(year)}',
        'fv_sizename': 'size',
        'fc_code': 'code toko',
        'fv_toko': 'toko',
        'fv_barcode':'artikel',
        'fn_total_sales_qty': f'total sales qty {str(year)}',
        'fn_stock_at': f'stock akhir {str(year)}',
        'fn_ts_all': f'total stock {str(year)}',
        'stock':'stock gudang'
    }

    # Gabungkan kedua dictionary
    rename_dict = {**month_columns, **other_columns}

    # Lakukan rename dan sort
    df = df.rename(columns=rename_dict)
    return df

def save_large_excel(df, path, sheet_prefix, chunk_size=50000):
    with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
        for i in range(0, len(df), chunk_size):
            df.iloc[i:i+chunk_size].to_excel(writer, sheet_name=f"{sheet_prefix}_{i//chunk_size + 1}", index=False)

def trx_rt():
    df_trxstx= df__rtt()
    df_trxstx.loc[df_trxstx['tgltrx'] == '10-30-0025', 'tgltrx'] = '10-30-2025'

    df_trxstx['tgltrx'] = pd.to_datetime(df_trxstx['tgltrx'], format='%m-%d-%Y')
    df_trxstx['fn_whconsiid']=df_trxstx['fn_whconsiid'].astype(int)
    df_trxstx['stock']=df_trxstx['stock'].astype(int)
    df_trxstx=df_trxstx.rename(columns={
        'stock':'fn_stock', 
        'tgltrx':'fd_tgltrx', 
        'stat':'fv_stat'
    })
    save_to_sql(df_trxstx, 'trxrt_pb', 'imp')
    # with get_engine('read') as engine:
    #         sql_types = define_sql_types(df_trxstx,'yes')
    #         df_trxstx.to_sql(f'trxstx_pb{str(year)[-2:]}', engine, if_exists='replace', index=False, dtype=sql_types)

def process_data_margin(df_trx, df_artimp, df_artred, df_atrxu, df_mrgincat, df_mrgin):
    # Merge data
    df_trxartcat = pd.merge(df_trx, df_artimp, on='fv_artsizecode', how='left')
    df_trxartcat = pd.merge(df_trxartcat, df_artred, on='fv_barcode', how='left')
    df_trxartcat = pd.merge(df_trxartcat, df_atrxu, on='fn_idurut', how='left')
    df_trxartcat["fn_markid"] = df_trxartcat["fn_markid"].combine_first(df_trxartcat["markid2"])
    df_trxartcat['fn_markid'] = df_trxartcat['fn_markid'].astype(int)
    df_trxartcat["fd_tgltrx"] = pd.to_datetime(df_trxartcat["fd_tgltrx"])
    
    # Merge with margin data
    merged_df = pd.merge(df_trxartcat, df_mrgincat, on=["fn_whconsiid", "fn_catid", "fn_markid"], how="left")
    filtered_df = merged_df[(merged_df["fd_tgltrx"] >= merged_df["fd_begindate"]) & (merged_df["fd_tgltrx"] <= merged_df["fd_enddate"])]
    df_mgcatfin = filtered_df[['fn_idurut', 'fn_margin']]
    
    merged_df2 = pd.merge(df_trxartcat, df_mrgin, on=["fn_whconsiid", "fn_markid"], how="left")
    filtered_df2 = merged_df2[merged_df2["fd_tgltrx"] >= merged_df2["fd_effectivedate"]]
    df_mgfin = filtered_df2.loc[filtered_df2.groupby(["fn_idurut", "fn_whconsiid", "fn_catid", "fn_markid"])["fd_effectivedate"].idxmax()]
    
    # Merge final margin data
    df_trx_final = df_trx[['fn_idurut']]
    df_trx_final = pd.merge(df_trx_final, df_mgcatfin, on="fn_idurut", how="left")
    df_trx_final = pd.merge(df_trx_final, df_mgfin[["fn_idurut", 'fn_margin']], on="fn_idurut", how="left", suffixes=("", "_from_mgfin"))
    df_trx_final["fn_margin"] = df_trx_final["fn_margin"].combine_first(df_trx_final["fn_margin_from_mgfin"])
    df_trx_final.drop(columns=["fn_margin_from_mgfin"], inplace=True, errors="ignore")
    df_trx_final = df_trx_final.fillna(0)
    df_trx_final = df_trx_final[~df_trx_final.duplicated()]
    df_trx_final['fc_status'] = np.where(df_trx_final['fn_margin'] == 0, 'X', 'O')
    df_trx_final2= pd.merge(df_trx_final, df_trxartcat[['fn_whconsiid','fn_markid','fn_idurut']], on='fn_idurut', how="left")
    df_trx_final3= df_mrgin.sort_values('fd_effectivedate', ascending=False)
    df_trx_final3= df_trx_final3.drop_duplicates(subset=['fn_whconsiid','fn_markid'], keep='first')
    df_trx_final3 = df_trx_final3.groupby('fn_whconsiid')['fn_margin'].agg(lambda x: x.mode().iloc[0]).reset_index()
    df_trx_final4= pd.merge(df_trx_final2, df_trx_final3, on=["fn_whconsiid"], how="left", suffixes=("", "_sim"))
    df_trx_final4['fn_margin'] = df_trx_final4.apply(lambda row: row['fn_margin_sim'] if row['fn_margin'] == 0 else row['fn_margin'], axis=1)
    return df_trx_final4

def get_artikel_md():
    # Load artikel data
    file_name = f'/opt/share/For MD/FOLLOW UP 2023 (4) (5).xlsx'
    df_nfa = pd.read_excel(file_name, sheet_name=8, header=5).dropna(how='all')
    df_nfa= df_nfa [['Article Code','Artikel/ Detail 1']]
    df_nfa=df_nfa.loc[~df_nfa['Article Code'].isnull()]
    df_nfa= df_nfa.loc[~df_nfa['Artikel/ Detail 1'].isnull()]
    df_nfa=df_nfa.rename(columns={'Article Code':'fv_barcode','Artikel/ Detail 1':'fv_neo_link_foto'})
    df_nfa=df_nfa.loc[~df_nfa['fv_barcode'].duplicated()]   
    return df_nfa    

def sls_disctok():
    df_simst,df_sidtl=df__sls_putus_disc()
    df_simmrg=pd.merge(df_simst,df_sidtl,how="inner",on='fc_nodoc')
    df_sidisctok=df_simmrg[df_simmrg['fm_totaldisc']!=df_simmrg['fm_totdisc']]
    df_sidisctok['disctok'] = np.where(
    df_sidisctok['fn_totalqty'] != 0, 
    np.ceil(df_sidisctok['fm_totaldisc'] / df_sidisctok['fn_totalqty']), 
    0)
    df_sidisctok= df_sidisctok.loc[~((df_sidisctok['fm_totdisc'] - df_sidisctok['fm_totaldisc'])>1)]
    # Mendapatkan nama kolom pertama dan terakhir
    col0 = df_sidisctok.columns[0]
    col_last = df_sidisctok.columns[-1]

    # Membuat DataFrame baru hanya dengan kolom pertama dan terakhir
    df_sidisctok = df_sidisctok[[col0, col_last]]
    return df_sidisctok

def get_new_or_changed_rows(df_baru: pd.DataFrame, df_lama: pd.DataFrame, subset: list):
    """
    Mengambil baris dari df_baru yang tidak ada di df_lama berdasarkan seluruh isi row dengan key dari subset.

    Parameters:
        subset: kolom unik, seperti 'id'
    """
    # Gabung df_lama dan df_baru berdasarkan subset (key) dengan suffix
    df_join = df_baru.merge(df_lama, on=subset, how='left', suffixes=('', '_lama'), indicator=True)

    # Baris yang tidak cocok atau ada perubahan isi
    kondisi_berubah = df_join['_merge'] == 'left_only'

    # Ambil kolom dari df_baru saja (hilangkan kolom *_lama dan _merge)
    kolom_asli = df_baru.columns
    return df_join.loc[kondisi_berubah, kolom_asli]   
def add_detail_tok(row):
    if row["fn_grpcusid"] == 2:
        return "MDS"
    elif row["fn_grpcusid"] == 3:
        return "RMY"
    elif row["fn_grpcusid"] > 3 and row["fc_consinetto"] == "F":
        return "Other"
    elif row["fn_grpcusid"] > 3 and row["fc_consinetto"] == "T":
        return "Consi"
    else:
        return None
def process_data_cus(df):
    """Memproses DataFrame sesuai aturan bisnis."""
    df = rename_col_py_to_sql(df).sort_values(by='fn_grpcusid')

    # Update kode untuk Surabaya
    df.loc[df['fv_kota'] == 'Surabaya', 'fc_code'] = df.loc[df['fv_kota'] == 'Surabaya', 'fc_code'].replace({'MD185': 'MD185.'})

    # Isi nilai null dengan nilai berurutan
    mask = df['fn_whconsiid'].isnull()
    df.loc[mask, 'fn_whconsiid'] = np.arange(9000, 9000 + mask.sum())
    # Isi `fc_codecus` yang null
    df['fc_codecus'] = df['fc_codecus'].fillna(df['fc_code'])
    # Tambahkan kondisi untuk `fv_configname`
    conditions = [18, 27, 74, 117, 134, 137, 151, 116]
    df.loc[df['fn_whconsiid'].isin(conditions), 'fv_configname'] = "Toko Strategis"
    df['fv_manager']=df['fv_manager'].replace({
        'HERU SULISTYO':'HERU S.',
        'HERU SULISTYO & JASON LEE':'HERU S. & JASON L.',
        'JERRY THIO':'JERRY T.'
    })
    # Buat kolom `fc_codecus` unik
    df['fc_codecus'] = df['fc_codecus'].astype(str) + df.groupby('fc_codecus').cumcount().add(1).astype(str).replace('1', '')
    

    df["fc_detail_tok"] = df.apply(add_detail_tok, axis=1)
    df.loc[(df['fv_neotoko'].isnull()) & (df['fc_code'].str.startswith('BZ')), 'fv_neotoko'] = "Bazar"
    df=df.rename(columns={'fv_configname':'fv_storetype'})
    return df

def upsert_data(engine, table_name, df, primary_key="fn_whconsiid"):
    with get_engine(engine) as engine:  # Menggunakan context manager dengan with
        engine = engine
    if df is None or df.empty:
        print("❌ DataFrame kosong! Upsert dibatalkan.")
        return

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Load metadata dan tabel
        metadata = MetaData()
        metadata.reflect(bind=engine)
        if table_name not in metadata.tables:
            raise Exception(f"Tabel '{table_name}' tidak ditemukan di database!")

        table = metadata.tables[table_name]
        db_columns = [col.name for col in table.columns]

        # Validasi kolom dari DataFrame
        df_columns = df.columns.tolist()
        missing_cols = set(df_columns) - set(db_columns)
        if missing_cols:
            raise Exception(f"Kolom tidak dikenali dalam tabel: {missing_cols}")

        # Replace NaN dengan None agar diterima MySQL
        df = df.where(pd.notnull(df), None)

        # Konversi df ke list of dict
        data = df.to_dict(orient="records")

        # Gunakan insert dari dialect MySQL
        stmt = mysql_insert(table).values(data)

        # Buat update_dict dari kolom yang ada di DataFrame (kecuali primary key)
        update_dict = {
            col: stmt.inserted[col]
            for col in df.columns
            if col != primary_key
        }

        # Buat UPSERT statement
        upsert_stmt = stmt.on_duplicate_key_update(**update_dict)

        # Eksekusi
        session.execute(upsert_stmt)
        session.commit()
        print(f"✅ Upsert berhasil: {len(df)} baris.")
    except Exception as e:
        session.rollback()
        error_message = f"❌ Gagal melakukan upsert:\n{traceback.format_exc()}\n"
        print(error_message)
        with open("upsert_error_log.txt", "a", encoding="utf-8") as f:
            f.write(error_message)
    finally:
        session.close()

def delete_rows(engine, table_name, df_filtered, primary_key="fn_whconsiid"):
    if df_filtered is None or df_filtered.empty:
        print("⚠️ Tidak ada data yang perlu dihapus.")
        return

    with get_engine(engine) as engine:
        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            metadata = MetaData()
            metadata.reflect(bind=engine)
            table = metadata.tables[table_name]

            # Ambil list primary key dari df_filtered
            keys_to_delete = df_filtered[primary_key].tolist()

            delete_stmt = table.delete().where(
                table.c[primary_key].in_(keys_to_delete)
            )

            session.execute(delete_stmt)
            session.commit()

            print(f"🗑️ Berhasil menghapus {len(keys_to_delete)} baris.")
        except Exception as e:
            session.rollback()
            print("❌ Error delete:", e)
        finally:
            session.close()

def add_ornal(row):
    acara = str(row.get('fv_namemark') or '')  # diasumsikan ini sama dengan Artikel[acara]
    kategori = row.get('fv_catname', '')
    harga = row.get('fm_amount', 0)
    # Acara diskon langsung jadi "Obral"
    if acara.startswith(("SP", "FP")) or acara in ["DISC 50%", "DISC 70%", "DISC 50+20%"]:
        return "Obral"
    elif acara.startswith("NP") or acara == "NORMAL":
        return "Normal"
    rules = {
        "LONG SLEEVE SHIRT": 229000,
        "SHORT SLEEVE SHIRT": 229000,
        "LONG SLEEVE KOKO SHIRT": 229000,
        "SHORT SLEEVE KOKO SHIRT": 229000,
        "SHORT SLEEVE POLO SHIRT": 189000,
        "SHORT SLEEVE HENLEY SHIRT": 189000,
        'SHORT SLEEVE UTILITIES SHIRT':189000,
        "SHORT SLEEVE T-SHIRT (CREW NECK)": 139000,
        "LONG SLEEVE T-SHIRT (CREW NECK)": 192000,
        "TRUCKER JACKET PIECE DYED": 300000,
        "TRUCKER JACKET DENIM":300000,
        'VARSITY JACKET':300000,
        "HERINGTON JACKET": 300000,
        "BOMBER JACKET": 300000,
        "UTILITIES JACKET": 300000,
        "HOODIE JACKET (KNIT)": 250000,
        "SWEAT SHIRT": 200000,
        "JOGER PANTS DENIM": 300000,
        "JOGER PANTS PIECE DYED": 300000,
        "JOGER SHORT DENIM": 279000,
        "5 POCKET PANT DENIM": 300000,
        "5 POCKET PANT PIECE DYED": 300000,
        "CHINO PANT PIECE DYED": 300000,
        "5 POCKET SHORT DENIM": 279000,
        "5 POCKET SHORT PIECE DYED": 279000,
        "CARGO PANT PIECE DYED": 300000,
        "CARGO SHORT PIECE DYED": 300000,
        "CHINO SHORT PIECE DYED": 279000,
        "CARGO PANT DENIM": 300000,
        "CARGO SHORT DENIM": 300000,
        "LONG SLEEVE UTILITIES SHIRT": 229000
    }

    batas_harga = rules.get(kategori)
    if batas_harga is not None:
        return "Normal" if harga >= batas_harga else "Obral"

    return None  # jika tidak ada kategori atau aturan
def proc_pivot_table_n(df,
                    values:str='stocks',
                    col_index:list=['fv_barcode', 'fn_whconsiid'],
                    columns:list=['fv_sizename'],
                    col_ket:list=['fv_configname'],col_select=None):
    
    all_sizes_bottom = ['25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38',
                        '39', '40', '42', '44', 'ALLSIZE']
    all_sizes_top = ['FS', 'XS', 'SS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL']
    all_sizes = all_sizes_bottom + all_sizes_top
    df_ket = df[col_index + col_ket].drop_duplicates()
    # Buat pivot table
    pivot_df = pd.pivot_table(
        df,
        values=values,
        index=col_index,
        columns=columns,
        aggfunc='sum',
        fill_value=0).reset_index()

    # Merge kembali kolom tambahan
    pivot_table = pd.merge(pivot_df, df_ket, on=col_index, how='left')
    for size in all_sizes:
        if size not in pivot_table.columns:
            pivot_table[size] = 0
        pivot_table[size] = pd.to_numeric(pivot_table[size], errors='coerce').fillna(0).astype(int)

    # Atur ulang urutan: non-size columns dulu, lalu size columns
    size_columns = [size for size in all_sizes if size in pivot_table.columns]
    other_columns = [col for col in pivot_table.columns if col not in size_columns]
    pivot_table = pivot_table[other_columns + size_columns]

    # Hitung status (BROKEN / SEHAT)
    top_columns = ["L", "M", "S", "XL", "XS", "XXL", "XXXL"]
    bottom_columns = ['25', '26', '27', '28', '29', '30', '31', '32', '33', '34',
                        '35', '36', '37', '38', '39', '40', '42', '44']

    pivot_table['status'] = np.where(
        (pivot_table["fv_configname"] == "TOP") & (pivot_table[top_columns].gt(0).sum(axis=1) < 3), "BROKEN",
        np.where(
            (pivot_table["fv_configname"] == "BOTTOM") & (pivot_table[bottom_columns].gt(0).sum(axis=1) < 7), "BROKEN",
            "SEHAT"
        )
    )

    # Hitung total jumlah qty size (sumQty)
    pivot_table['sumQty'] = pivot_table[size_columns].sum(axis=1)
    pivot_table=pivot_table.drop(columns=['fv_configname'])
    if col_select==None:
        return pivot_table
    else : return pivot_table[col_select]

def proc_add_aging(df,df_aging):
### Proses untuk menambahkan kolom '1st DO', 'last_receive', dan 'stock_on_1st_do' ke DataFrame df.
    if not df_aging.empty:
        # Group df_aging by fn_whconsiid and fv_barcode, and get the minimum fd_tgltrx
        aging_min_date = df_aging.groupby(['fn_whconsiid', 'fv_barcode'])['fd_tgltrx'].min().reset_index()
        aging_min_date.rename(columns={'fd_tgltrx': '1st DO'}, inplace=True)

        # Group df_aging by fn_whconsiid and fv_barcode, and get the maximum fd_tgltrx (last_receive)
        aging_max_date = df_aging.groupby(['fn_whconsiid', 'fv_barcode'])['fd_tgltrx'].max().reset_index()
        aging_max_date.rename(columns={'fd_tgltrx': 'last_receive'}, inplace=True)

        # Merge df with aging_min_date on fn_whconsiid and fv_barcode
        df = pd.merge(df, aging_min_date, on=['fn_whconsiid', 'fv_barcode'], how='left')
        df = pd.merge(df, aging_max_date, on=['fn_whconsiid', 'fv_barcode'], how='left')

        # Get stock on 1st DO
        # Merge aging_df with aging_min_date to get stock on 1st DO
        aging_stock_on_1st_do = pd.merge(
            df_aging,
            aging_min_date,
            on=['fn_whconsiid', 'fv_barcode'],
            how='left'
        )

        # Filter rows where fd_tgltrx matches the 1st DO
        aging_stock_on_1st_do = aging_stock_on_1st_do[
        aging_stock_on_1st_do['fd_tgltrx'] == aging_stock_on_1st_do['1st DO']]

        # Select only the required columns
        aging_stock_on_1st_do = aging_stock_on_1st_do[['fn_whconsiid', 'fv_barcode', 'fn_stock']]
        # aging_stock_on_1st_do.rename(columns={'fn_stock': 'stock_on_1st_do'}, inplace=True)

        # Merge df with aging_stock_on_1st_do to add stock_on_1st_do column
        df = pd.merge(df, aging_stock_on_1st_do, on=['fn_whconsiid', 'fv_barcode'], how='left')
        df_sum = df.groupby(['fn_whconsiid', 'fv_barcode'], as_index=False)['fn_stock'].sum().drop_duplicates(subset=['fv_barcode'])
        df = pd.merge(df, df_sum, on=['fn_whconsiid', 'fv_barcode'], how='left')
        df = df.rename(columns={'fn_stock_y': 'stock_on_1st_do'}).drop(columns=['fn_stock_x'])
        df['stock_on_1st_do'] = df['stock_on_1st_do'].fillna(0)
        # Convert '1st DO' to datetime
        df['1st DO'] = pd.to_datetime(df['1st DO'])
        df['last_receive'] = pd.to_datetime(df['last_receive'])

        # Hitung aging (selisih hari dari last_receive ke hari ini)
        df['aging'] = (pd.Timestamp.today().normalize() - df['last_receive']).dt.days.fillna(0).astype(int)

        # Format '1st DO' to 'YYYY-MM-DD'
        df['1st DO'] = df['1st DO'].dt.strftime('%Y-%m-%d')
        df['last_receive'] = df['last_receive'].dt.strftime('%Y-%m-%d')
        return df
    else:
        # Jika aging_df kosong, buat kolom dengan nilai default
        df['1st DO'] = '2024-01-01'
        df['last_receive'] = '2024-01-01'
        df['aging'] = '2024-01'
        df['stock_on_1st_do'] = 0
        return df
def proc_pivot_table(df):
    all_sizes_bottom = ['25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38',
                        '39', '40', '42', '44', 'ALLSIZE']
    all_sizes_top = ['FS', 'XS', 'SS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL']
    all_sizes = all_sizes_bottom + all_sizes_top

    # Buat pivot table
    pivot_table = pd.pivot_table(
        df,
        values='stocks',
        index=['fv_barcode', 'fn_whconsiid'],
        columns=['fv_sizename'],
        aggfunc='sum',
        fill_value=0
    )

    # Reset index agar bisa merge
    pivot_table = pivot_table.reset_index()

    # Kolom detail yang ingin ditambahkan kembali
    kolom_detail = ['fv_barcode', '1st DO', 'last_receive', 'aging','fv_configname',
                    'stock_on_1st_do', 'fn_whconsiid']
    # Merge detail
    pivot_table = pd.merge(pivot_table, df[kolom_detail], on=['fv_barcode', 'fn_whconsiid'], how='left')
    pivot_table = pivot_table.drop_duplicates()

    # Urutkan kolom agar kolom ukuran di akhir
    urutan_kolom = kolom_detail + [col for col in pivot_table.columns if col not in kolom_detail]
    pivot_table = pivot_table[urutan_kolom]

    # Pastikan semua ukuran muncul
    for size in all_sizes:
        if size not in pivot_table.columns:
            pivot_table[size] = 0
        pivot_table[size] = pd.to_numeric(pivot_table[size], errors='coerce').fillna(0).astype(int)

    # Atur ulang urutan: non-size columns dulu, lalu size columns
    size_columns = [size for size in all_sizes if size in pivot_table.columns]
    other_columns = [col for col in pivot_table.columns if col not in size_columns]
    pivot_table = pivot_table[other_columns + size_columns]

    # Hitung status (BROKEN / SEHAT)
    top_columns = ["L", "M", "S", "XL", "XS", "XXL", "XXXL"]
    bottom_columns = ['25', '26', '27', '28', '29', '30', '31', '32', '33', '34',
                        '35', '36', '37', '38', '39', '40', '42', '44']

    pivot_table['status'] = np.where(
        (pivot_table["fv_configname"] == "TOP") & (pivot_table[top_columns].gt(0).sum(axis=1) < 3), "BROKEN",
        np.where(
            (pivot_table["fv_configname"] == "BOTTOM") & (pivot_table[bottom_columns].gt(0).sum(axis=1) < 5), "BROKEN",
            "SEHAT"
        )
    )

    # Hitung total jumlah qty size (sumQty)
    pivot_table['sumQty'] = pivot_table[all_sizes].sum(axis=1)
    pivot_table=pivot_table.drop(columns=['fv_configname'])

    return pivot_table
    
MONTH_MAP = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
    'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
    'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}

import re

def parse_year_month(file_path):
    match = re.search(r'pb_stock_(\w+)_(\d{2})\.xlsx', file_path.lower())
    if not match:
        return None, None

    month_str, year_short = match.groups()
    year = int('20' + year_short)
    month = MONTH_MAP[month_str]

    return year, month
def generate_stock_files_parquet(base_path, start_year, end_year, months=None):
    """
    months: None | int | list[int]
    """
    stock_files = []

    MONTH_LIST = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                  'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

    # normalisasi months
    if months is None:
        month_indices = range(1, 13)
    elif isinstance(months, int):
        month_indices = [months]
    else:
        month_indices = months

    for year in range(start_year, end_year + 1):
        for m in month_indices:
            month_str = MONTH_LIST[m - 1]
            file_path = os.path.join(
                base_path,
                f'pb_stock_{month_str}_{str(year)[-2:]}.xlsx'
            )
            stock_files.append(file_path)

    return stock_files

def convert_to_local_path(path: str) -> str:
    """
    Konversi path acuan Airflow (/opt/share/...) ke Windows Network Share
    (\\\\192.168.1.254\\Share\\_From Valdi\\...) saat berjalan di lingkungan Windows lokal.
    """
    if not path:
        return path
    if os.name == 'nt':
        norm = str(path).replace('\\\\', '/').replace('\\', '/')
        converted = None
        if norm.startswith('/opt/share'):
            rel = norm[len('/opt/share'):].strip('/')
            rel_win = rel.replace('/', '\\')
            converted = r'\\192.168.1.254\Share\_From Valdi' + ('\\' + rel_win if rel_win else '')
        elif norm.lower().startswith('c:/opt/share'):
            rel = norm[len('c:/opt/share'):].strip('/')
            rel_win = rel.replace('/', '\\')
            converted = r'\\192.168.1.254\Share\_From Valdi' + ('\\' + rel_win if rel_win else '')

        if converted and converted != path:
            print(f"🔄 [AUTO-CONVERT] Path dialihkan ke Windows Share: {converted}")
            return converted
    return path

def get_stock_data_parquet(parquet_base, start_year, end_year, months=None):
    """
    Membaca dan menggabungkan data stock bulanan dari parquet secara teroptimasi.
    months: None | int | list[int]
    """
    import pyarrow.parquet as pq
    parquet_base = convert_to_local_path(parquet_base)
    all_data = []

    # normalisasi bulan (mendukung 1 s.d. 13 untuk kebutuhan stock akhir tahun / Power BI)
    if months is None:
        month_list = range(1, 14)
    elif isinstance(months, int):
        month_list = [months]
    else:
        month_list = [m for m in months if 1 <= m <= 13]

    for year in range(start_year, end_year + 1):
        year_suffix = str(year)[-2:]
        for month in month_list:
            parquet_path = os.path.join(
                parquet_base,
                f"tahun={year}",
                f"bulan={str(month).zfill(2)}",
                "stock.parquet"
            )

            if os.path.exists(parquet_path):
                print(f"✅ Membaca parquet: {parquet_path}")
                # Column projection: baca schema lebih dulu agar hanya kolom relevan yang dimuat ke RAM
                schema = pq.read_schema(parquet_path)
                stock_cols = [name for name in schema.names if name.startswith("stock ")]
                cols_to_read = ['fn_whconsiid', 'fv_artsizecode'] + stock_cols

                df = pd.read_parquet(parquet_path, columns=cols_to_read)
                
                # Optimasi tipe data untuk menghemat memori & mempercepat groupby
                df['fn_whconsiid'] = df['fn_whconsiid'].astype('int32')
                for sc in stock_cols:
                    df[sc] = df[sc].astype('float32')

                # Logika aman untuk bulan 13 (Saldo Akhir Tahun / Stock At):
                # Apapun nama kolom stok di bulan 13 (misal 'stock jan_26', 'stock jan_27', dll),
                # rename menjadi 'stock at_{year_suffix}' agar tidak bertabrakan dengan bulan lain.
                if month == 13:
                    stock_at_name = f"stock at_{year_suffix}"
                    rename_m13 = {sc: stock_at_name for sc in stock_cols if sc != stock_at_name}
                    if rename_m13:
                        df = df.rename(columns=rename_m13)
                        print(f"  [INFO] Kolom bulan 13 {list(rename_m13.keys())} di-mapping ke {list(rename_m13.values())} (Stock Akhir Tahun).")

                all_data.append(df)
            else:
                print(f"❌ Parquet tidak ditemukan: {parquet_path}")

    if not all_data:
        print("⚠️ Tidak ada data parquet yang terbaca.")
        return pd.DataFrame()

    combined_data = pd.concat(all_data, ignore_index=True)
    print("Sebelum groupby:", combined_data.shape)

    # pastikan kolom ada
    required_cols = {'fn_whconsiid', 'fv_artsizecode'}
    if required_cols.issubset(combined_data.columns):
        combined_data = (
            combined_data
            .groupby(['fn_whconsiid', 'fv_artsizecode'], as_index=False)
            .sum(numeric_only=True)
        )
        print("Setelah groupby:", combined_data.shape)
    else:
        print("⚠️ Kolom wajib tidak lengkap:", required_cols)

    return combined_data

def get_nodo_tglreceive(import_sql: str,year):
    import_sql = import_sql.lower()
    if import_sql not in ['yes', 'no']:
        raise ValueError("Parameter 'import_sql' harus berupa 'yes' atau 'no'.")

    base_path = r'/opt/share'
    found_file = None

    # Mapping bulan bahasa Indonesia ke bahasa Inggris (3 huruf & variasi)
    month_map = {
        'jan': 'Jan', 'januari': 'Jan', 'january': 'Jan',
        'feb': 'Feb', 'februari': 'Feb', 'pebruari': 'Feb', 'february': 'Feb',
        'mar': 'Mar', 'maret': 'Mar', 'march': 'Mar',
        'apr': 'Apr', 'april': 'Apr',
        'mei': 'May', 'may': 'May',
        'jun': 'Jun', 'juni': 'Jun', 'june': 'Jun',
        'jul': 'Jul', 'juli': 'Jul', 'july': 'Jul',
        'agu': 'Aug', 'agt': 'Aug', 'agustus': 'Aug', 'aug': 'Aug', 'august': 'Aug',
        'sep': 'Sep', 'september': 'Sep',
        'okt': 'Oct', 'oktober': 'Oct', 'oct': 'Oct', 'october': 'Oct',
        'nop': 'Nov', 'nopember': 'Nov', 'nov': 'Nov', 'november': 'Nov',
        'des': 'Dec', 'desember': 'Dec', 'dec': 'Dec', 'december': 'Dec'
    }

    def clean_tracking_filename(filename: str) -> str:
        """
        Membersihkan format tanggal di nama file, contoh:
        (5-okt-2026)  -> (05-Oct-2026)
        (05-Okt-2026) -> (05-Oct-2026)
        (1-Sep-2026)  -> (01-Sep-2026)
        (31-Agu-2026) -> (31-Aug-2026)
        """
        pattern = r'\(\s*(\d{1,2})\s*[-/\s]\s*([a-zA-Z]+)\s*[-/\s]\s*(\d{4})\s*\)'
        match = re.search(pattern, filename)
        if not match:
            return filename
        day_str, month_str, year_str = match.groups()
        month_clean = month_map.get(month_str.lower(), month_str.capitalize()[:3])
        day_clean = f"{int(day_str):02d}"
        clean_date_bracket = f"({day_clean}-{month_clean}-{year_str})"
        return filename[:match.start()] + clean_date_bracket + filename[match.end():]

    # ==========================================================
    # Cleaning & Rename File jika format tanggal belum standar
    # ==========================================================
    if os.path.exists(base_path):
        for fname in os.listdir(base_path):
            if fname.startswith(f"TRACKING RESI {year}") and fname.endswith(".xlsx"):
                clean_name = clean_tracking_filename(fname)
                if clean_name != fname:
                    old_path = os.path.join(base_path, fname)
                    new_path = os.path.join(base_path, clean_name)
                    try:
                        if not os.path.exists(new_path):
                            os.rename(old_path, new_path)
                            print(f"File berhasil di-rename (clean): {fname} -> {clean_name}")
                        else:
                            print(f"File standar sudah ada ({clean_name}), menggunakan file tersebut.")
                    except Exception as e:
                        print(f"Peringatan: Gagal rename {fname} menjadi {clean_name}: {e}")

    # =========================
    # Cari file (mundur 7 hari)
    # =========================
    for i in range(7):
        check_date = (datetime.today() - timedelta(days=i)).strftime("%d-%b-%Y")
        candidate = fr"{base_path}/TRACKING RESI {year} ({check_date}).xlsx"
        if os.path.exists(candidate):
            found_file = candidate
            print(f"File ditemukan: {candidate}")
            break

    # Fallback jika file belum/tidak bisa di-rename tapi cocok setelah dinormalisasi
    if not found_file and os.path.exists(base_path):
        for i in range(7):
            check_date = (datetime.today() - timedelta(days=i)).strftime("%d-%b-%Y")
            target_name = f"TRACKING RESI {year} ({check_date}).xlsx"
            for fname in os.listdir(base_path):
                if fname.startswith(f"TRACKING RESI {year}") and fname.endswith(".xlsx"):
                    if clean_tracking_filename(fname) == target_name:
                        found_file = os.path.join(base_path, fname)
                        print(f"File ditemukan (via normalisasi): {found_file}")
                        break
            if found_file:
                break

    if not found_file:
        return (
            f"Tidak ada file TRACKING RESI {year} untuk 7 hari terakhir."
        )
    
    # print('test')
    # =========================
    # Baca Excel
    # =========================
    xl = pd.ExcelFile(found_file)
    sheet_names = xl.sheet_names
    required_cols = [
        'No. DO',
        'Date of Receipt',
        'Description',
        'No. Tracking Number'
    ]
    dfs = []
    def read_sheet(sheet_name):
        df = (
            pd.read_excel(found_file, sheet_name=sheet_name, header=3)
              .dropna(how='all')
        )

        missing_cols = set(required_cols) - set(df.columns)
        if missing_cols:
            raise ValueError(
                f"Sheet '{sheet_name}' tidak memiliki kolom: {missing_cols}"
            )
        return df[required_cols]

    # sheet terakhir (wajib)
    dfs.append(read_sheet(sheet_names[-1]))

    # sheet sebelum terakhir (jika ada)
    if len(sheet_names) > 1:
        dfs.append(read_sheet(sheet_names[-2]))

    df_combi = pd.concat(dfs, ignore_index=True)
    df_combi['Description'] = df_combi['Description'].astype(str).str.split(',').str[0]
    # =========================
    # Ambil data existing SQL
    # =========================
    df_doa = df__date_arrival()
    # print('test')
    # =========================
    # Gabung & Bersihkan
    # =========================
    df_final = pd.concat([df_doa, df_combi], ignore_index=True)

    def clean_indo_date(val):
        if pd.isna(val) or isinstance(val, (datetime, pd.Timestamp)):
            return val
        s = str(val).strip()
        m = re.match(r'^(\d{1,2})[-/\s]([a-zA-Z]+)[-/\s](\d{4})$', s)
        if m:
            d, mon, y = m.groups()
            mon_en = month_map.get(mon.lower(), mon.capitalize()[:3])
            return f"{int(d):02d}-{mon_en}-{y}"
        return val

    df_final['Date of Receipt'] = df_final['Date of Receipt'].apply(clean_indo_date)
    df_final['Date of Receipt'] = pd.to_datetime(
        df_final['Date of Receipt'],
        errors='coerce'
    )
    df_final = (
        df_final
        # .sort_values('Date of Receipt')
        .drop_duplicates(subset=['No. DO'], keep='last')
        [['No. DO', 'Date of Receipt', 'Description', 'No. Tracking Number']]
    )
    # Pembaharuan do baru dan lama
    df_doread = df__domst_tb('fc_nodoc',distinct=False,where=f"and fc_nodoc like 'DO%%'")
    # df_doread = pd.read_sql(
    # "SELECT fc_nodoc FROM domst_tb where and fc_nodoc like 'DO%%'",
    # con=engine2
    # )
    df_doread=df_doread[df_doread['fc_nodoc'].str.count('/')==4]
    df_doread[['No. DO','user']]=df_doread['fc_nodoc'].str.rsplit('/',n=1,expand=True)
    df_doread['user']='/'+df_doread['user']
    df_final=df_final.merge(df_doread[['No. DO','fc_nodoc']], how='left',on='No. DO')
    df_final['No. DO']=np.where(df_final['fc_nodoc'].isna(),df_final['No. DO'],df_final['fc_nodoc'])
    df_final=df_final.drop(columns='fc_nodoc')
    df_final=df_final.rename({'Description':'fv_description'})
    # =========================
    # Simpan ke SQL (opsional)
    # =========================

    save_to_sql(df_final, 'date_arrival_do', 'imp')
    # if import_sql == 'yes':
    #     df_final.to_sql(
    #         'date_arrival_do',
    #         engine,
    #         if_exists='replace',
    #         index=False
    #     )

    return df_final


def get_penjualan_tabel(year):
    df_table=df__penju_tabel(year)
    df_art=df_artikel_pb_("fv_artsizecode,fv_divname Divisi")
    df_cus=df__customers_pb_("fn_whconsiid,fc_code 'kode Toko',fv_toko Toko")
    df_sum=df_cus.merge(df_table,how='right',on='fn_whconsiid')
    df_sum=df_sum.merge(df_art,how='left',on='fv_artsizecode')
    df_sum['tgltrx'] = pd.to_datetime(df_sum['tgltrx'], format='%m-%d-%Y')
    df_sum['Bulan'] = df_sum['tgltrx'].dt.strftime('%m')
    df_sum['Nama Bulan'] = df_sum['tgltrx'].dt.strftime('%B')
    df_sum['Tahun'] = df_sum['tgltrx'].dt.year
    mapping_tabel = {
        1: "Counter",
        2: "Location 1",
        3: "Location 2",
        4: "Location 3",
        5: "Location 4",
        6: "Bazaar",
        7: "Borongan",
        8: "Online",
        9: "Table",
        11: "Fair",
    }

    # 2. Tambahkan kolom baru ke DataFrame utama (asumsi nama dataframe adalah df)
    df_sum["Lokasi"] = df_sum["fn_tabel"].map(mapping_tabel)
    df=df_sum
    # Asumsi `df` adalah DataFrame utama Anda

    # 1. Daftar semua Lokasi baru sesuai urutan
    all_locations = [
        "Counter",
        "Location 1",
        "Location 2",
        "Location 3",
        "Location 4",
        "Bazaar",
        "Borongan",
        "Online",
        "Table",
        "Fair",
    ]

    # 2. Definisikan Styling Excel
    font_header = Font(name="Calibri", size=10, bold=True, color="FFFFFF")
    fill_header = PatternFill(
        start_color="1F4E78", end_color="1F4E78", fill_type="solid"
    )  # Navy Dark

    font_data = Font(name="Calibri", size=9)
    font_bold = Font(name="Calibri", size=9, bold=True)

    fill_zebra = PatternFill(
        start_color="F9FAFB", end_color="F9FAFB", fill_type="solid"
    )
    fill_total = PatternFill(
        start_color="E2EFDA", end_color="E2EFDA", fill_type="solid"
    )  # Hijau Muted Total

    thin_border_side = Side(border_style="thin", color="D9D9D9")
    border_cell = Border(
        left=thin_border_side,
        right=thin_border_side,
        top=thin_border_side,
        bottom=thin_border_side,
    )

    thick_top = Side(border_style="thin", color="000000")
    double_bottom = Side(border_style="double", color="000000")
    border_total = Border(
        top=thick_top,
        bottom=double_bottom,
        left=thin_border_side,
        right=thin_border_side,
    )

    # WRAP TEXT pada Header diaktifkan
    align_header = Alignment(horizontal="center", vertical="center", wrap_text=True)
    align_left = Alignment(horizontal="left", vertical="center")
    align_center = Alignment(horizontal="center", vertical="center")
    align_right = Alignment(horizontal="right", vertical="center")

    # 3. Grouping data per Tahun dan Bulan
    grouped = df.groupby(["Tahun", "Bulan", "Nama Bulan"])

    for (tahun, bulan, namabulan), group in grouped:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = f"Laporan {namabulan}"
        ws.views.sheetView[0].showGridLines = True

        # Pivot Table Data
        pt = pd.pivot_table(
            group,
            index=["Toko", "Divisi", "Obral/Normal"],
            columns="Lokasi",
            values=["Total_Rupiah", "Qty"],
            aggfunc="sum",
            fill_value=0,
        )

        # Filter lokasi yang ada pada data bulan tersebut
        present_locations = [
            loc for loc in all_locations if loc in pt.columns.levels[1]
        ]

        # --- Header Level 1 & 2 ---
        ws.cell(row=1, column=1, value="Toko")
        ws.cell(row=1, column=2, value="Divisi")
        ws.cell(row=1, column=3, value="Obral / Normal")
        ws.cell(row=1, column=4, value="Total Sales Rp")
        ws.cell(row=1, column=5, value="Total Sales Qty")

        ws.merge_cells("A1:A2")
        ws.merge_cells("B1:B2")
        ws.merge_cells("C1:C2")
        ws.merge_cells("D1:D2")
        ws.merge_cells("E1:E2")

        current_col = 6  # Dimulai dari kolom F
        loc_col_start = {}

        for loc in present_locations:
            loc_col_start[loc] = current_col
            ws.cell(row=1, column=current_col, value=loc)
            ws.merge_cells(
                start_row=1,
                start_column=current_col,
                end_row=1,
                end_column=current_col + 1,
            )

            ws.cell(row=2, column=current_col, value="Sales Rp")
            ws.cell(row=2, column=current_col + 1, value="Sales Qty")
            current_col += 2

        max_col = current_col - 1

        # Apply Style Header (Row 1 & Row 2) dengan Wrap Text
        for r in range(1, 3):
            for c in range(1, max_col + 1):
                cell = ws.cell(row=r, column=c)
                cell.font = font_header
                cell.fill = fill_header
                cell.alignment = align_header
                cell.border = border_cell

        # --- Populate Data Rows ---
        indices = pt.index.tolist()
        row_idx = 3

        for idx in indices:
            toko, divisi, obral_norm = idx

            ws.cell(row=row_idx, column=1, value=toko).alignment = align_left
            ws.cell(row=row_idx, column=2, value=divisi).alignment = align_center
            ws.cell(row=row_idx, column=3, value=obral_norm).alignment = (
                align_center
            )

            # Formula Total Sales Rp (Kolom F ke kanan, kelipatan 2)
            rp_cols = [get_column_letter(c) for c in range(6, max_col + 1, 2)]
            sum_rp_formula = (
                f"=SUM(" + ",".join([f"{col}{row_idx}" for col in rp_cols]) + ")"
            )
            ws.cell(row=row_idx, column=4, value=sum_rp_formula).number_format = (
                "#,##0"
            )
            ws.cell(row=row_idx, column=4).alignment = align_right
            ws.cell(row=row_idx, column=4).font = font_bold

            # Formula Total Sales Qty (Kolom G ke kanan, kelipatan 2)
            qty_cols = [get_column_letter(c) for c in range(7, max_col + 1, 2)]
            sum_qty_formula = (
                f"=SUM(" + ",".join([f"{col}{row_idx}" for col in qty_cols]) + ")"
            )
            ws.cell(row=row_idx, column=5, value=sum_qty_formula).number_format = (
                "#,##0"
            )
            ws.cell(row=row_idx, column=5).alignment = align_right
            ws.cell(row=row_idx, column=5).font = font_bold

            # Isi Data per Lokasi
            for loc in present_locations:
                c_start = loc_col_start[loc]

                sales_rp = (
                    pt.loc[idx, ("Total_Rupiah", loc)]
                    if ("Total_Rupiah", loc) in pt.columns
                    else 0
                )
                sales_qty = (
                    pt.loc[idx, ("Qty", loc)] if ("Qty", loc) in pt.columns else 0
                )

                c_rp = ws.cell(row=row_idx, column=c_start, value=sales_rp)
                c_rp.number_format = "#,##0"
                c_rp.alignment = align_right

                c_qty = ws.cell(row=row_idx, column=c_start + 1, value=sales_qty)
                c_qty.number_format = "#,##0"
                c_qty.alignment = align_right

            # Zebra striping
            is_even = row_idx % 2 == 0
            for c in range(1, max_col + 1):
                cell = ws.cell(row=row_idx, column=c)
                cell.font = (
                    font_data
                    if c > 5
                    else (font_bold if c in [4, 5] else font_data)
                )
                if is_even:
                    cell.fill = fill_zebra
                cell.border = border_cell

            row_idx += 1

        # --- Baris Grand Total ---
        ws.cell(row=row_idx, column=1, value="Grand Total").alignment = align_left
        ws.cell(row=row_idx, column=1).font = font_bold
        ws.merge_cells(
            start_row=row_idx, start_column=1, end_row=row_idx, end_column=3
        )

        for c in range(4, max_col + 1):
            col_letter = get_column_letter(c)
            total_formula = f"=SUM({col_letter}3:{col_letter}{row_idx-1})"
            cell = ws.cell(row=row_idx, column=c, value=total_formula)
            cell.number_format = "#,##0"
            cell.font = font_bold
            cell.alignment = align_right

        for c in range(1, max_col + 1):
            cell = ws.cell(row=row_idx, column=c)
            cell.fill = fill_total
            cell.border = border_total

        # --- Autofit Column Width Khusus berdasarkan ISI DATA (Row 3+) untuk Kolom A, B, C, D, E ---
        for col in ws.columns:
            col_idx = col[0].column
            col_letter = get_column_letter(col_idx)
            max_len = 0

            if col_idx in [1, 2, 3, 4, 5]:
                # HANYA hitung dari baris ke-3 sampai bawah (mengabaikan teks header row 1 & 2)
                for cell in col[2:]:
                    if cell.value is not None:
                        val_str = str(cell.value)
                        val_len = (
                            14 if val_str.startswith("=") else len(val_str)
                        )  # Estimasi panjang angka formula
                        if val_len > max_len:
                            max_len = val_len
                ws.column_dimensions[col_letter].width = max(max_len + 4, 11)
            else:
                # Kolom F ke kanan (Lokasi): Hitung seluruh isi termasuk header
                for cell in col:
                    if cell.value is not None:
                        val_str = str(cell.value)
                        val_len = 12 if val_str.startswith("=") else len(val_str)
                        if val_len > max_len:
                            max_len = val_len
                ws.column_dimensions[col_letter].width = max(max_len + 0, 12)

        # Freeze Panes disesuaikan ke 'F3'
        ws.freeze_panes = "F3"

        # Simpan ke file per bulan
        filename = fr"/opt/share/For MKT ADMIN/Laporan Penjualan By Table/Laporan Penjualan By Table {bulan} {tahun} {namabulan}.xlsx"
        wb.save(filename)
        print(f"File tersimpan: {filename}")
            # df_sum_sum=df_sum_sum[[]]
def get_stock_transit():
    df_ots_=df__otsdmdo_rt()
    df_rtr=df__ots_retur_gd()
    df_rtr=df_rtr.loc[~df_rtr['fv_artsizecode'].isnull()]
    df_combined = pd.concat([df_ots_[['fv_artsizecode', 'fn_qty']], 
                         df_rtr[['fv_artsizecode', 'fn_qty']]], 
                        ignore_index=True)
    # 2. Groupby berdasarkan fv_artsizecode dan jumlahkan fn_qty
    df_result = df_combined.groupby('fv_artsizecode', as_index=False)['fn_qty'].sum()
    # list_art=list_to_sql_in(df_result,'fv_artsizecode')
    # df_art = df_artikel_pb_('fv_artsizecode, fv_barcode', where=f'and fv_artsizecode in ({list_art})')
    # df_result=df_result.merge(df_art,how='left',on='fv_artsizecode')
    # df_result=df_result.groupby('fv_barcode', as_index=False)['fn_qty'].sum()
    df_result=df_result.rename(columns={
                                "fn_qty":"Stock Transit",
                                "fv_barcode":"artikel"})
    return df_result
