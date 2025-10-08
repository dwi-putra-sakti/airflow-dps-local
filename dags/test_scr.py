
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date
import calendar
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy import Integer, String, CHAR, Date, Text, text, Float
import os
# Informasi koneksi

host2 = "172.16.11.9"
port2 = 3307
database2 = "gis_local"

host = "36.93.148.91"
port = 3307
user = "dataanalyst"
password = "PyData*010224"
database = "gis_db1"

DATABASE_TYPE = 'mysql+pymysql'
@contextmanager
def get_engine(stat):
    """Context manager untuk membuka dan menutup SQLAlchemy engine."""
    if stat == 'read':
        engine = create_engine(f'{DATABASE_TYPE}://{user}:{password}@{host}:{port}/{database}')
    else:
        engine = create_engine(f'{DATABASE_TYPE}://{user}:{password}@{host2}:{port2}/{database2}')

    try:
        # Mengembalikan engine untuk digunakan dalam blok with
        yield engine
    finally:
        # Menutup koneksi pool engine untuk membersihkan sumber daya
        engine.dispose()

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
        'ukuran': 'size',
        'fc_code': 'code toko',
        'fv_toko': 'toko',
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

def md_artikel_size2():
    with get_engine('imp') as engine_imp:
        query='''
        SELECT fc_code , fv_toko, fn_whconsiid FROM gis_local.customers_pb
        '''
        df_cus= pd.read_sql(query, engine_imp)
        query='''
        select 
        fv_barcode artikel, fv_artsizecode barcode,
        fv_sizename ukuran
        from artikel_pb
        '''
        df_artsq= pd.read_sql(query, engine_imp)
        query='''
        SELECT * FROM gis_local.sales_thru_pb_24 
        '''
        df_sqq24= pd.read_sql(query, engine_imp)
        query='''
        SELECT * FROM gis_local.sales_thru_pb_25 
        '''
        df_sqq25= pd.read_sql(query, engine_imp)
        query='''
        SELECT fv_artsizecode, stock FROM gis_local.stock_gudang_pb where fv_whtypename in ('RETUR','PUSAT','PUTUS','QC') group by fv_artsizecode
        '''
        df_sg= pd.read_sql(query, engine_imp)

    df_sq24=df_sqq24.copy()
    df_sq25=df_sqq25.copy()
    df_sq24=df_sq24.drop(columns=[df_sq24.columns[1]])
    df_sq25=df_sq25.drop(columns=[df_sq25.columns[1]])
    df_sq24['fn_artsizeid']=df_sq24['fn_artsizeid'].astype(str)
    df_sq25['fn_artsizeid']=df_sq25['fn_artsizeid'].astype(str)
    df_sq24 = df_sq24.groupby(['fv_artsizecode','fn_artsizeid'])[df_sq24.columns[2:]].sum().reset_index()
    df_sq25 = df_sq25.groupby(['fv_artsizecode','fn_artsizeid'])[df_sq25.columns[2:]].sum().reset_index()

    # Buat dictionary rename berdasarkan data
    df_sq24 = generate_rename_dict(2024, 2024, df_sq24)
    df_sq25 = generate_rename_dict(2025, 2025, df_sq25)
    df=pd.merge(df_sq24,df_sq25,how='outer',on=['fn_artsizeid','fv_artsizecode'])
    df['qty retur']=df['qty retur_x'].fillna(0)+df['qty retur_y'].fillna(0)
    df['stock akhir']=df['stock akhir_y']
    df['total sales qty']=df['total sales qty_x'].fillna(0)+df['total sales qty_y'].fillna(0)
    df['total stock']=df['total sales qty']+df['stock akhir'].fillna(0)
    df=df.drop(columns=['qty retur_x','qty retur_y','total stock_x', 'stock akhir_x', 'stock akhir_y',
                        'total stock_y','total sales qty_x','total sales qty_y'])
    df_sg['fv_artsizecode']=df_sg['fv_artsizecode'].str.upper()
    df = pd.merge(df, df_sg, left_on='fv_artsizecode', right_on='fv_artsizecode', how='outer')
    df = pd.merge(df, df_artsq, left_on='fv_artsizecode', right_on='barcode', how='left')
    df = df.loc[~(df['artikel'].isnull())]
    df2=df.fillna(0)
    df2=df2[df2['artikel']!=0]
    kolom_baru24 = ['artikel', 'ukuran'] + [col for col in df2.columns if col not in ['artikel', 'ukuran','barcode','fv_artsizecode','fn_artsizeid']]
    df2 = df2[kolom_baru24]
    df2r=df2.select_dtypes(include='number').sum()
    df2r.to_excel('artsize_gd_result.xlsx')
    df2.to_excel(fr'\\192.168.1.254\Share\From Valdi\PB DB\stock artikel all 2025--.xlsx', index=False)

def calculate_duration(start, end):
    """Menghitung durasi antara dua waktu."""
    duration = (end - start).total_seconds() / 60
    return f"{int(duration)} menit" if duration >= 1 else f"{int(duration * 60)} detik"

def main():
    timestart_md_artikel_size= datetime.now()
    print("[INFO] Script md_artikel_size dimulai.")
    print(f"[INFO] Timestart: {timestart_md_artikel_size}\n")
    md_artikel_size2()
    timefin= datetime.now()
    total_duration = calculate_duration(timestart_md_artikel_size, timefin)

    logs = [("Finish", timestart_md_artikel_size, timefin)]

    file_name = timefin.strftime("output_%Y%m%d_%H%M%S.txt")
    with open(file_name, "w") as file:
        for label, start, end, *optional_duration in logs:
            formatted_start = start.strftime('%Y-%m-%d %H:%M:%S')
            if end:
                duration_formatted = calculate_duration(start, end)
                file.write(f"{label}: {formatted_start} ({duration_formatted})\n")
            else:
                total_info = f" (Total: {optional_duration[0]})" if optional_duration else ""
                file.write(f"{label}: {formatted_start}{total_info}\n")
if __name__ == "__main__":
    main()