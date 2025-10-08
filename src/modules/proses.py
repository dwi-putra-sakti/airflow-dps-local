from modules.queries import (df__tglso, df__tglsa, df__disc2, df__margin, df__artsizecode, df__stock_so,
                             df__stock_sa, df__retur, df__size, generate_months_for_year, df__rtt,
                             df__sls_putus_disc, df__aging, df__customers_pb)
from modules.db_connection import get_engine
from sqlalchemy import Integer, String, CHAR, DateTime, Text, text, Float, Numeric
import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy import MetaData
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import sessionmaker
import traceback
import os

def clean_and_transform_data(df):
    """Melakukan pembersihan dan transformasi data."""
    df['tgltrx'] = pd.to_datetime(df['tgltrx'], format='%m-%d-%Y')
    df['bulan'] = df['tgltrx'].dt.month
    df['nama_bulan'] = df['tgltrx'].dt.strftime('%B')
    df['tahun'] = df['tgltrx'].dt.year

    df['fv_artsizecode'] = df['fv_artsizecode'].str.upper()
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

    }
    # Lakukan rename dan sort
    df = df.rename(columns=name_col)
    return df

def insert_pk(df):
    df_artsizecode = df__artsizecode()
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
        col_dtype = df[col].dtype

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
        df.to_sql(table_name, con=engine, if_exists='replace', index=False, dtype=sql_types)
from sqlalchemy import text

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
    combined_data = pd.merge(stock_data, sales_data, on=['fn_whconsiid', 'fv_artsizecode'], how='outer').fillna(0)
    # combined_data=combined_data.drop(columns='tgltrx')
    # Generate nama kolom stok dan qty berdasarkan tahun
    periods = generate_months_for_year(start_year, end_year)  # Gunakan function yang baru dibuat
    months = list(periods.keys())  # Ambil nama bulan dari dict

    # Hitung total stok per bulan
    for i, month in enumerate(months):
        next_month_stock = f'stock {months[i+1]}' if i < len(months) - 1 else None
        qty_col = f'qty_{month}'
        combined_data[f'total_stock_{month}'] = combined_data.get(next_month_stock, 0) + combined_data.get(qty_col, 0)

    # Hitung total kuantitas dan total stok
    qty_cols = [f'qty_{m}' for m in months]
    stock_cols= [f'stock {m}' for m in months]
    # Cari kolom stok yang ada dalam data
    available_stock_cols = [col for col in stock_data.columns if col.startswith('stock ')]

    # Pilih kolom stok terakhir (sesuai urutan bulan)
    if available_stock_cols:
        last_stock_col = (available_stock_cols)[-1]  # Ambil stok terakhir berdasarkan nama kolom
    else:
        last_stock_col = None  # Jika tidak ada stok, gunakan None
        
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

    # Bersihkan data (hapus baris jika semua kolom = 0)
    combined_data['min'] = combined_data[stock_cols + ['total_stock']+['qty']].min(axis=1)
    combined_data['max'] = combined_data[stock_cols + ['total_stock']+['qty']].max(axis=1)
    combined_data = combined_data[(combined_data['min'] != 0) | (combined_data['max'] != 0)]
    return combined_data

def merge_detail(final_data, stock_data, start_year, end_year):
    df_articlesize = df__artsizecode()
    df_size = df__size()

    final_data = pd.merge(final_data, df_articlesize, on='fv_artsizecode', how='left')
    final_data = pd.merge(final_data, df_size, on='fn_sizeid', how='left')
    final_data = final_data[~final_data['fv_sizename'].isnull()]

    # Membuat kolom secara dinamis untuk semua bulan di rentang tahun
    columns = ['fn_artsizeid', 'fn_whconsiid', 'fv_artsizecode']
    rename_columns = {'qty': 'fn_qty_retur'}
    month_names = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

    current_year = datetime.now().year
    current_month = datetime.now().month +1
    # last_stock_col = None  # Variabel untuk menyimpan kolom stock terakhir

    for year in range(start_year, end_year + 1):
        last_month = current_month if year == current_year else 12  # Sampai bulan saat ini di tahun berjalan

        for month in range(1, last_month + 1):
            month_str = f"{month_names[month-1]}_{str(year)[-2:]}"  # Format jan_25, feb_25, dst.
            col_prefix = f'{month:02d}'
            
            stock_col = f'stock {month_str}'

            # Hanya menyimpan stock bulan terakhir
            if month == last_month:
                last_stock_col = stock_col
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
                columns[:-1]
                available_stock_cols = [col for col in stock_data.columns if col.startswith('stock ')]

                # Pilih kolom stok terakhir (sesuai urutan bulan)
                if available_stock_cols:
                    last_stock_col = (available_stock_cols)[-1]  # Ambil stok terakhir berdasarkan nama kolom
                else:
                    last_stock_col = None  # Jika tidak ada stok, gunakan None
                
                total_stock_dec = f'total_stock_dec_{str(year)[-2:]}'
                qty_dec = f'qty_dec_{str(year)[-2:]}'
                stock_dec=f'stock dec_{str(year)[-2:]}'
                columns.extend([total_stock_dec, qty_dec,last_stock_col])
                rename_columns.update({
                    stock_dec:'fn_stock_12',
                    total_stock_dec: 'fn_ts_12',
                    qty_dec: 'fn_qty_12',
                    last_stock_col:'fn_stock_at'
                })
    # additional condition
    columns.extend([])
    columns.extend(['qty', 'total_stock', 'total_qty'])
    rename_columns.update({
        'total_qty': 'fn_total_sales_qty',
        'total_stock': 'fn_ts_all'
    })
    final_data = final_data[columns]
    final_data = final_data.rename(columns=rename_columns).sort_values(by='fn_whconsiid')
    return final_data

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
        'fn_qty_retur': 'qty retur',
        'fv_sizename': 'size',
        'fc_code': 'code toko',
        'fv_toko': 'toko',
        'fv_barcode':'artikel',
        'fn_total_sales_qty': 'total sales qty',
        'fn_stock_at': 'stock akhir',
        'fn_ts_all': 'total stock',
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
    df.loc[df['fn_whconsiid'].isin(conditions), 'fv_configname'] = "7 Toko Strategis"
    df['fv_manager']=df['fv_manager'].replace({
        'HERU SULISTYO':'HERU S.',
        'HERU SULISTYO & JASON LEE':'HERU S. & JASON L.',
        'JERRY THIO':'JERRY T.'
    })
    # Buat kolom `fc_codecus` unik
    df['fc_codecus'] = df['fc_codecus'].astype(str) + df.groupby('fc_codecus').cumcount().add(1).astype(str).replace('1', '')
    

    df["fc_detail_tok"] = df.apply(add_detail_tok, axis=1)
    df.loc[(df['fv_neotoko'].isnull()) & (df['fc_code'].str.startswith('BZ')), 'fv_neotoko'] = "Bazar"
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

def add_ornal(row):
    acara = row.get('fv_status', '')  # diasumsikan ini sama dengan Artikel[acara]
    kategori = row.get('fv_catname', '')
    harga = row.get('fm_amount', 0)

    # Acara diskon langsung jadi "Obral"
    if acara in ["DISC 50%", "DISC 70%", "DISC 50+20%"]:
        return "Obral"
    if acara in ["NORMAL"]:
        return "Normal"
    rules = {
        "LONG SLEEVE SHIRT": 229000,
        "SHORT SLEEVE SHIRT": 229000,
        "LONG SLEEVE KOKO SHIRT": 229000,
        "SHORT SLEEVE KOKO SHIRT": 229000,
        "SHORT SLEEVE POLO SHIRT": 189000,
        "SHORT SLEEVE HENLEY SHIRT": 189000,
        "SHORT SLEEVE T-SHIRT (CREW NECK)": 139000,
        "LONG SLEEVE T-SHIRT (CREW NECK)": 192000,
        "TRUCKER JACKET PIECE DYED": 300000,
        "TRUCKER JACKET DENIM":300000,
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
            (pivot_table["fv_configname"] == "BOTTOM") & (pivot_table[bottom_columns].gt(0).sum(axis=1) < 5), "BROKEN",
            "SEHAT"
        )
    )

    # Hitung total jumlah qty size (sumQty)
    pivot_table['sumQty'] = pivot_table[size_columns].sum(axis=1)
    pivot_table=pivot_table.drop(columns=['fv_configname'])
    if col_select==None:
        return pivot_table
    else : return pivot_table[col_select]

def proc_add_aging(df):
    
    df_aging = df__aging()
    df_cus=df__customers_pb('fn_whconsiid,fc_code "code toko"')
    # results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    # df = pd.DataFrame(results)
###     Proses untuk menambahkan kolom '1st DO', 'last_receive', dan 'stock_on_1st_do' ke DataFrame df.
    if not df_aging.empty:
        # Group df_aging by fn_whconsiid and fv_barcode, and get the minimum fd_tgltrx
        aging_min_date = df_aging.groupby(['fn_whconsiid', 'fv_barcode'])['fd_tgltrx'].min().reset_index()
        aging_min_date.rename(columns={'fd_tgltrx': '1st DO'}, inplace=True)

        # Group df_aging by fn_whconsiid and fv_barcode, and get the maximum fd_tgltrx (last_receive)
        aging_max_date = df_aging.groupby(['fn_whconsiid', 'fv_barcode'])['fd_tgltrx'].max().reset_index()
        aging_max_date.rename(columns={'fd_tgltrx': 'last_receive'}, inplace=True)
        df= pd.merge(df, df_cus, on='code toko', how='left')
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
        df=df.rename(columns={'artikel':'fv_barcode'})
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

        df=df.rename(columns={'fv_barcode':'artikel'})
        return df
    else:
        # Jika aging_df kosong, buat kolom dengan nilai default
        df['1st DO'] = '2024-01-01'
        df['last_receive'] = '2024-01-01'
        df['aging'] = 0
        df['stock_on_1st_do'] = 0
        return df
