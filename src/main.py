import pandas as pd
import numpy as np
import gc
import math
from datetime import date, timedelta
import calendar
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy import Integer, String, CHAR, Date, Text, text, Float
from dateutil.relativedelta import relativedelta
import os
from datetime import datetime, date
from modules.db_connection import get_engine, get_db
from modules.queries import (df__penju, df__tokoawal, df__transaction, df__cus, get_monthly_sales_data, 
                             df__sales_thru_24, df__sales_thru_25, df__sales_thru_25_count_artikel, df_tglsosa,
                             df__artsizecode, df__gdmd, fetch_data_margin,list_to_sql_in,df__sales_thru_26,
                             df__sales_, df__customers_pb, df__artikel_pb, df__artikel_pb_read, 
                             df__tgl_input_penju,df__stock_today_all_store,df__stock_wh,df__sales_ptsbz,
                             df__customers_pb_read, df__cus_user, df__sales_invoice,  df__sales_bz_disc2nd,
                             df__sales_bz, df__sales_consi_disc2nd, df__sales_online_disc2nd, df__user,
                             df__otsdm, df__otsdo,df__pb_penjualan_,df__customers_pb_, df_artikel_pb_, 
                             df__otsob,df__aging,df__otsdo_rt,df__aging_pb_,df__aging_gudang_pb,
                             df__penju_today, df__artikel_status, df__tglso_, df__tglsa_, df__whtype_name, 
                             df__aging_gudang , df__fstock_wh,df__stockbz_post,df__stockbz_now,df__onbook_stock,
                             df__penju_bz,df__transaction_bz,df_tglsosa_bz, df__tracking_rm, df__tracking_do,
                             df__date_arrival,df__detail_tracking,df__draft_pending, df__sales_pmse) 
from modules.proses import (save_to_sql, add_index_and_foreign_keys, clean_and_transform_data, 
                            insert_disc2_grouping, insert_margin, rename_col_py_to_sql, insert_pk,
                            get_tgl, get_stock, get_stock_data, merge_and_calculate, merge_detail, 
                            generate_rename_dict,trx_rt, process_data_margin,rename_col_sql_to_xlsx_gis,
                            get_artikel_md, get_new_or_changed_rows,upsert_data, process_data_cus,delete_rows,
                            sls_disctok, proc_pivot_table_n, add_ornal,ensure_stock_columns,
                            get_penjualan_tabel,get_stock_transit,
                            proc_add_aging,proc_pivot_table,get_stock_data_parquet,get_nodo_tglreceive,
                            convert_to_local_path)

def margin_tb(year):
    df_trx, df_artimp, df_artred, df_atrxu, df_mrgincat, df_mrgin = fetch_data_margin(year)
    df_trx_final = process_data_margin(df_trx, df_artimp, df_artred, df_atrxu, df_mrgincat, df_mrgin)
    save_to_sql(df_trx_final, f'margin{str(year)[-2:]}_pb', 'imp')

def sales_to_sql(year):
    start_time2= datetime.now().strftime('%Y-%m-%d %H:%M:%S')    
    print("\n[INFO] Script sales_to_sql mulai dijalankan.")
    print(f"[INFO] Timestamp: {start_time2}\n")
    disc2nd_consi_online()
    df1= df__penju(year)
    df_bz=df__penju_bz(year)
    df_bz['fn_idurut'] += 100000000
    df1=pd.concat([df1,df_bz], ignore_index=True)
    df= df1.copy()
    df= clean_and_transform_data(df)
    df=insert_disc2_grouping(df,'sum')
    # margin_tb(year)
    df=insert_margin(df,year)
    sum_col=['fn_jualpersize','total_penjualan2','total_disc2']
    agg_col=['fn_whconsiid', 'fv_artsizecode', 'tgltrx']
    fisrt_col=[col for col in df.columns if col not in sum_col + agg_col]
    col_col=fisrt_col+[col for col in df.columns if col not in fisrt_col]
    df=df[col_col]
    df['event']=df['event'].fillna(df['status_artikel'])
    df['fn_margin']=df['fn_margin'].fillna(0)
    df=rename_col_py_to_sql(df).sort_values(by='fn_idurut')
    df=insert_pk(df)
    df['fn_nett_amount'] = (df['fn_totalpenjualan']-df['fn_total_disc2']) * (1-df['fn_margin']/100)
    df['fn_sim_amount'] = np.where(df['fc_status'] == "X", df['fn_totalpenjualan'], df['fn_nett_amount'])
    df['fn_dpp'] = df['fn_nett_amount']/1.11
    df['fn_dpp2'] = df['fn_sim_amount']/1.11
    # df_tglimput=df__tgl_input_penju(year)
    
    df_art=df_artikel_pb_('fv_artsizecode, fv_catname',distinct=False,where='')
    df=df.merge(df_art,how='left',on='fv_artsizecode')
    df=df.rename(columns={'fn_hargasatuan':'fm_amount','fv_status_artikel':'fv_status'})
    df['fv_ornal'] = df.apply(add_ornal, axis=1)
    df=df.rename(columns={'fm_amount':'fn_hargasatuan','fv_status':'fv_status_artikel'}).drop(columns='fv_catname')
    
    # df=df.merge(df_tglimput, how='left',on='fv_nopenjualan')
    table_name = f"pb_penjualan_{year}"
    save_to_sql(df, table_name, 'imp')

    add_index_and_foreign_keys(
        'imp',
        table_name,
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
                "ref_table": "artikel_tb",
                "ref_column": "fv_artsizecode",
                "constraint_name": f"fk_sales_artsize_pb_{year}"
            }
        ]
    )

def get_stock_monthly(tglEND,expot_excel):
    '''Main Stock Monthly'''
    tglEND2= datetime.strptime(tglEND, "%Y-%m-%d").date()
    tglEND = datetime.strptime(tglEND, '%Y-%m-%d').strftime('%Y-%m-%d 23:59:59')
    # Dapatkan tanggal kemarin
    today = datetime.now()
    d2ago = today - timedelta(days=2)
    today2 = datetime.now().strftime('%Y-%m-%d')
    # Ubah ke string (format default: YYYY-MM-DD)
    # kemarin_str = kemarin.strftime("%Y-%m-%d")
    # d2ago_str = d2ago.strftime("%Y-%m-%d")
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
    # expot_excel = expot_excel.lower()
    tglnow = datetime.today().date() # Tanggal Perhari Ini 
    df_tokoawal = df__tokoawal()
    idwhtoko_list_awal = df_tokoawal['fn_whconsiid'].tolist() # Ubah kedalam List
    ## tanggal so/sa DATE_FORMAT(fd_tglinject, '%%Y-%%m-%%d') AS fd_tglflagIndo
    df_tglLSO=df_tglsosa(idwhtoko_list_awal,today2)
    df_tglLSO['fd_tglflagIndo'] = pd.to_datetime(df_tglLSO['fd_tglflagIndo'])
    df_tglLSO = df_tglLSO.loc[
        ~(
            (df_tglLSO['fd_tglflagIndo'] < tglEND) &
            (df_tglLSO['fc_hold'] == 'T')   )]
    
    df_tglLSO=df_tglsosa(idwhtoko_list_awal,tglEND)
    df_tglLSO['fd_tglflagIndo'] = pd.to_datetime(df_tglLSO['fd_tglflagIndo'])
    if tglEND2 > tglnow:  # Jika tanggal yang dipilih lebih besar dari hari ini
        df_tglLSO.loc[df_tglLSO['fc_status']=="T",'fd_tglflagIndo'] -= pd.Timedelta(days=1)

    df_tglLSO['fd_tglflagIndo'] = df_tglLSO['fd_tglflagIndo'].dt.strftime('%Y-%m-%d')
    df_tglLSO=df_tglLSO.loc[~df_tglLSO['fd_tglflagIndo'].isna()]
    idwhtoko_list = df_tglLSO['fn_whconsiid'].tolist() # Ubah kedalam List untuk kode toko
    tgl_list = df_tglLSO['fd_tglflagIndo'].tolist() # Ubah kedalam List untuk tanggal SO terakhir
    fv_stat=df_tglLSO['fv_stat'].tolist()

    # Mencari Stock pertanggal yang dipilih dengan menjumlah stock awal SO terakhir dengan transaksi dari SO terakhir sampai tanggal yang dipilih
    df_tok = pd.DataFrame() # Dataframe Kosong
    for idwhtoko, tglSTR, stat in zip(idwhtoko_list, tgl_list,fv_stat): # Looping untuk masing-masing toko dan tanggal SO terakhir
        df_transactions = df__transaction(idwhtoko, tglSTR, tglEND,stat) # Mencari semua transaksi
        df_transactions['fv_artsizecode'] = df_transactions['fv_artsizecode'].str.upper()
        df_transactions['fv_artsizecode'] = df_transactions['fv_artsizecode'].replace({'BBRD103V042333-': 'BBRD103V042333'})
        df_transactions['fn_whconsiid'] = idwhtoko 
        df_transactions['tgltrx'] = tglEND
        df_tok = pd.concat([df_tok, df_transactions], ignore_index=True)
    fdate= str(year)[2:4]
    df_tok=df_tok.loc[~((df_tok['fn_whconsiid'].isin([1131,1130,1129]))&(df_tok['qty']==0))]
    df_tok= df_tok[['fn_whconsiid','fv_artsizecode','qty','tgltrx']].rename(columns={'qty':f'stock {bulan}_{fdate}'})
    
    if expot_excel == 'yes':
        max_rows = 1048570
        output_file = f'/opt/share/PB DB/pb_stock_{bulan}_{fdate}.xlsx'
        print(len(df_tok))
        print(max_rows)
        if len(df_tok) <= max_rows:
            # Simpan 1 sheet saja
            df_tok.to_excel(output_file, index=False)
        else:
            # Potong menjadi 2 bagian
            df1 = df_tok.iloc[:max_rows]
            df2 = df_tok.iloc[max_rows:]
            print(df1.info())
            print(df2.info())
            with pd.ExcelWriter(output_file) as writer:
                df1.to_excel(writer, sheet_name='Sheet1', index=False)
                df2.to_excel(writer, sheet_name='Sheet2', index=False)
        print(f'pb_stock_{bulan}_{fdate} Selesai')
        return df_tok
    print(f'pb_stock_{bulan}_{fdate} Selesai')
    return df_tok

def merge_stock_by_date(tgl_input):
    """
    Menggabungkan stock_dept dan stock_bz berdasarkan input tanggal 'YYYY-MM-DD'.
    """
    try:
        # 1. Parsing tanggal
        dt = datetime.strptime(tgl_input, '%Y-%m-%d')
        next_month_dt = dt + relativedelta(months=1)
        year_str = str(next_month_dt.year)
        month_str = str(next_month_dt.month).zfill(2)

        print(f"[INFO] Memproses data untuk Tahun: {year_str}, Bulan: {month_str}")

        # 2. Definisi Path Input (Gunakan f-string untuk fleksibilitas)
        base_input = "/opt/share/PB DB/stock"
        # base_input = r'\\192.168.1.254\Share\_From Valdi\PB DB\stock'
        path_dept = os.path.join(base_input, f"tahun={year_str}", f"bulan={month_str}", "stock_dept.parquet")
        path_bz = os.path.join(base_input, f"tahun={year_str}", f"bulan={month_str}", "stock_bz.parquet")

        # 3. Baca Data
        # Gunakan pengecekan file agar tidak error jika file tidak ditemukan
        if not os.path.exists(path_dept) or not os.path.exists(path_bz):
            print(f"❌ File tidak ditemukan di path: {year_str}/{month_str}")
            return

        df_dept = pd.read_parquet(path_dept)
        df_bz = pd.read_parquet(path_bz)

        # 4. Gabungkan
        df = pd.concat([df_dept, df_bz], ignore_index=True)

        # 5. Definisi Path Output
        parquet_base_out = "/opt/share/PB DB/stock"
        output_path = os.path.join(parquet_base_out, f"tahun={year_str}", f"bulan={month_str}")
        
        os.makedirs(output_path, exist_ok=True)
        output_file = os.path.join(output_path, "stock.parquet")

        # 6. Simpan
        df.to_parquet(output_file, engine="pyarrow", index=False)
        
        print(f"✅ Berhasil! Parquet saved: {output_file}")
        return output_file

    except Exception as e:
        print(f"⚠️ Terjadi kesalahan: {e}")

# # Cara Pakai:
# merge_stock_by_date('2026-04-01')

def write_stock_parquet(df, parquet_base, bz=False):
    """
    parquet_base/
      tahun=YYYY/
        bulan=MM/
          stock.parquet
    """
    year = int(df['tahun'].iloc[0])
    month = int(df['bulan'].iloc[0])

    output_path = os.path.join(
        parquet_base,
        f"tahun={year}",
        f"bulan={str(month).zfill(2)}"
    )
    os.makedirs(output_path, exist_ok=True)

    if bz==True:
        output_file = os.path.join(output_path, "stock_bz.parquet")
    else:
        output_file = os.path.join(output_path, "stock_dept.parquet")

    df.to_parquet(
        output_file,
        engine="pyarrow",
        index=False
    )

    print(f"✅ Parquet saved: {output_file}")

def get_stock_monthly_parquet(tglEND,expot_excel):
    '''Main Stock Monthly'''
    tglEND2= datetime.strptime(tglEND, "%Y-%m-%d").date()
    tglEND = datetime.strptime(tglEND, '%Y-%m-%d').strftime('%Y-%m-%d 23:59:59')
    # Dapatkan tanggal kemarin
    today = datetime.now()
    d2ago = today - timedelta(days=2)
    # Ubah ke string (format default: YYYY-MM-DD)
    # kemarin_str = kemarin.strftime("%Y-%m-%d")
    # d2ago_str = d2ago.strftime("%Y-%m-%d")
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
    # expot_excel = expot_excel.lower()
    tglnow = datetime.today().date() # Tanggal Perhari Ini 
    df_tokoawal = df__tokoawal()
    idwhtoko_list_awal = df_tokoawal['fn_whconsiid'].tolist() # Ubah kedalam List
    ## tanggal so/sa DATE_FORMAT(fd_tglinject, '%%Y-%%m-%%d') AS fd_tglflagIndo
    df_tglLSO=df_tglsosa(idwhtoko_list_awal,tglEND) 
    df_tglLSO['fd_tglflagIndo'] = pd.to_datetime(df_tglLSO['fd_tglflagIndo'])

    df_tglLSO = df_tglLSO.loc[
        ~(
            (df_tglLSO['fd_tglflagIndo'] < tglEND) &
            (df_tglLSO['fc_hold'] == 'T')   )]
    
    if tglEND2 > tglnow:  # Jika tanggal yang dipilih lebih besar dari hari ini
        df_tglLSO.loc[df_tglLSO['fc_status']=="T",'fd_tglflagIndo'] -= pd.Timedelta(days=1)

    df_tglLSO['fd_tglflagIndo'] = df_tglLSO['fd_tglflagIndo'].dt.strftime('%Y-%m-%d')
    df_tglLSO=df_tglLSO.loc[~df_tglLSO['fd_tglflagIndo'].isna()]
    idwhtoko_list = df_tglLSO['fn_whconsiid'].tolist() # Ubah kedalam List untuk kode toko
    tgl_list = df_tglLSO['fd_tglflagIndo'].tolist() # Ubah kedalam List untuk tanggal SO terakhir
    fv_stat=df_tglLSO['fv_stat'].tolist()

    # Mencari Stock pertanggal yang dipilih dengan menjumlah stock awal SO terakhir dengan transaksi dari SO terakhir sampai tanggal yang dipilih
    df_tok = pd.DataFrame() # Dataframe Kosong
    for idwhtoko, tglSTR, stat in zip(idwhtoko_list, tgl_list,fv_stat): # Looping untuk masing-masing toko dan tanggal SO terakhir
        df_transactions = df__transaction(idwhtoko, tglSTR, tglEND,stat) # Mencari semua transaksi
        df_transactions['fv_artsizecode'] = df_transactions['fv_artsizecode'].str.upper()
        df_transactions['fv_artsizecode'] = df_transactions['fv_artsizecode'].replace({'BBRD103V042333-': 'BBRD103V042333'})
        df_transactions['fn_whconsiid'] = idwhtoko 
        df_transactions['tgltrx'] = tglEND
        df_tok = pd.concat([df_tok, df_transactions], ignore_index=True)
    fdate= str(year)[2:4]
    # df_tok=df_tok.loc[~((df_tok['fn_whconsiid'].isin([1131,1130,1129]))&(df_tok['qty']==0))]
    # df_tokobz = df__customers_pb_('fn_whconsiid',where='and fc_code like "BZ%%" and fc_hold="F"')
    # list_toko_bz=list_to_sql_in(df_tokobz,'fn_whconsiid','str')
    # df_bzr_post=df__stockbz_post('a.fn_whconsiid, fv_artsizecode, fn_qtyskrg qty',where=f'and fn_bulan={month} and fn_tahun={year} and a.fn_whconsiid in ({list_toko_bz})')
    # df_bzr_now=df__stockbz_now('fn_whconsiid, fv_artsizecode, fn_qty qty')
    # df_tok=pd.concat([df_tok, df_bzr_post,df_bzr_now], ignore_index=True).fillna({'tgltrx':tglEND})
    df_tok=df_tok[['fn_whconsiid','fv_artsizecode','qty','tgltrx']].rename(columns={'qty':f'stock {bulan}_{fdate}'})
    df_tok['tahun'] = year
    df_tok['bulan'] = month
    if expot_excel == 'yes':
        max_rows = 1048570
        output_file = f'/opt/share/PB DB/pb_stock_{bulan}_{fdate}.xlsx'
        print(len(df_tok))
        print(max_rows)
        if len(df_tok) <= max_rows:
            # Simpan 1 sheet saja
            df_tok.to_excel(output_file, index=False)
        else:
            # Potong menjadi 2 bagian
            df1 = df_tok.iloc[:max_rows]
            df2 = df_tok.iloc[max_rows:]
            print(df1.info())
            print(df2.info())
            with pd.ExcelWriter(output_file) as writer:
                df1.to_excel(writer, sheet_name='Sheet1', index=False)
                df2.to_excel(writer, sheet_name='Sheet2', index=False)
        print(f'pb_stock_{bulan}_{fdate} Selesai')
        return df_tok
    elif expot_excel == 'parquet':
        parquet_base = "/opt/share/PB DB/stock"
        write_stock_parquet(df_tok, parquet_base)

    print(f'pb_stock_{bulan}_{fdate} Selesai')
    return df_tok

def get_stock_monthly_parquet_bz(tglEND,expot_excel):
    '''Main Stock Monthly'''
    tglEND2= datetime.strptime(tglEND, "%Y-%m-%d").date()
    tglEND = datetime.strptime(tglEND, '%Y-%m-%d').strftime('%Y-%m-%d 23:59:59')
    # Ubah ke string (format default: YYYY-MM-DD)
    # kemarin_str = kemarin.strftime("%Y-%m-%d")
    # d2ago_str = d2ago.strftime("%Y-%m-%d")
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
    today = datetime.today() 
    tglnow = today.strftime('%Y-%m-%d 23:59:59')
    # tglEND = '2025-08-31'
    # tglEND = '2025-09-30'
    df_tokoawal = df__customers_pb_('fn_whconsiid',
                                    where='and fc_code like "bz%%" and fc_code not in ("BZ013","BZ023","BZ005") and fc_hold="F"')
    idwhtoko_list_awal = df_tokoawal['fn_whconsiid'].tolist() # Ubah kedalam List
    df_tglLSO=df_tglsosa_bz(idwhtoko_list_awal,tglnow)
    df_tglLSO = df_tglLSO.loc[
            ~(
                (df_tglLSO['fd_tglflagIndo'] < tglEND) &
                (df_tglLSO['fc_hold'] == 'T')   )]
    toko_buka_tglEND=df_tglLSO.dropna(subset='fd_tglflag')['fn_whconsiid'].tolist()
    df_tglLSO_END=df_tglsosa_bz(toko_buka_tglEND,tglEND)
    df_tglLSO_END['fd_tglflagIndo'] = df_tglLSO_END['fd_tglflagIndo'].fillna('2022-01-01')
    idwhtoko_list = df_tglLSO_END['fn_whconsiid'].tolist() # Ubah kedalam List untuk kode toko
    tgl_list = df_tglLSO_END['fd_tglflagIndo'].tolist() # Ubah kedalam List untuk tanggal SO terakhir
    fv_stat=df_tglLSO_END['fv_stat'].tolist()
    df_tok = pd.DataFrame() # Dataframe Kosong
    for idwhtoko, tglSTR, stat in zip(idwhtoko_list, tgl_list,fv_stat): # Looping untuk masing-masing toko dan tanggal SO terakhir
        df_transactions = df__transaction_bz(idwhtoko, tglSTR, tglEND,stat) # Mencari semua transaksi
        df_transactions['fv_artsizecode'] = df_transactions['fv_artsizecode'].str.upper()
        df_transactions['fv_artsizecode'] = df_transactions['fv_artsizecode'].replace({'BBRD103V042333-': 'BBRD103V042333'})
        df_transactions['fn_whconsiid'] = idwhtoko 
        df_transactions['tgltrx'] = tglEND
        df_tok = pd.concat([df_tok, df_transactions], ignore_index=True)

    fdate= str(year)[2:4]
    df_tok=df_tok.loc[~((df_tok['fn_whconsiid'].isin([1131,1130,1129]))&(df_tok['qty']==0))]
    df_tok=df_tok[['fn_whconsiid','fv_artsizecode','qty','tgltrx']].rename(columns={'qty':f'stock {bulan}_{fdate}'})
    df_tok['tahun'] = year
    df_tok['bulan'] = month
    if expot_excel == 'yes':
        max_rows = 1048570
        output_file = f'/opt/share/PB DB/pb_stock_{bulan}_{fdate}.xlsx'
        print(len(df_tok))
        print(max_rows)
        if len(df_tok) <= max_rows:
            # Simpan 1 sheet saja
            df_tok.to_excel(output_file, index=False)
        else:
            # Potong menjadi 2 bagian
            df1 = df_tok.iloc[:max_rows]
            df2 = df_tok.iloc[max_rows:]
            print(df1.info())
            print(df2.info())
            with pd.ExcelWriter(output_file) as writer:
                df1.to_excel(writer, sheet_name='Sheet1', index=False)
                df2.to_excel(writer, sheet_name='Sheet2', index=False)
        print(f'pb_stock_{bulan}_{fdate} Selesai')
        return df_tok
    elif expot_excel == 'parquet':
        parquet_base = "/opt/share/PB DB/stock"
        write_stock_parquet(df_tok, parquet_base, bz=True)

    print(f'pb_stock_{bulan}_{fdate} Selesai')
    return df_tok

def get_last_three_months_including_current(typefile='excel'):
    timestart = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("[INFO] Script get_last_three_months_including_current dimulai.")
    print(f"[INFO] Timestart: {timestart}\n")

    today = datetime.now()
    last_dates = []

    for i in range(2, -1, -1):  # Loop mundur dari 2 bulan lalu sampai bulan ini
        target_month = (today.month - i - 1) % 12 + 1
        target_year = today.year - ((today.month - i - 1) < 0)

        # Dapatkan hari terakhir bulan tersebut
        last_day = calendar.monthrange(target_year, target_month)[1]
        last_dates.append(f"{target_year}-{target_month:02d}-{last_day:02d}")
    if typefile== "parquet":
        for tgl in last_dates:
            print(tgl)
            get_stock_monthly_parquet(tgl, typefile)
            get_stock_monthly_parquet_bz(tgl, typefile)
            merge_stock_by_date(tgl)
    else:
        for tgl in last_dates:
            get_stock_monthly(tgl, 'yes')
        # Print tanggal yang dipilih sebelum return
    print("[INFO] Tanggal yang dipilih:")
    print("\n".join(last_dates))

    return last_dates

def stock_and_sales(start_year, end_year, typefile='parquet'):
    import time
    t_start = time.time()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("\n[INFO] Script stock_and_sales mulai dijalankan.")
    print(f"[INFO] Timestamp: {timestamp}")
    print(f"[INFO] Periode: {start_year} - {end_year} | Tipe File: {typefile}\n")

    # Deteksi path otomatis (Linux Docker vs Windows UNC Share)
    if os.name == 'nt':
        base_path = r'\\192.168.1.254\Share\_From Valdi\PB DB'
        parquet_base = r'\\192.168.1.254\Share\_From Valdi\PB DB\stock'
    else:
        base_path = '/opt/share/PB DB/'
        parquet_base = '/opt/share/PB DB/stock'

    print(f"[PATH] Base path: {base_path} (exists: {os.path.exists(base_path)})")
    print(f"[PATH] Parquet base: {parquet_base} (exists: {os.path.exists(parquet_base)})")

    # 1. Load Data Stock
    t0 = time.time()
    if typefile == 'parquet':
        stock_data = get_stock_data_parquet(parquet_base, start_year, end_year)
    else:
        stock_data = get_stock_data(base_path, start_year, end_year)
    print(f"[1/5] Load stock data selesai ({stock_data.shape[0]:,} baris) dalam {time.time() - t0:.2f} detik")

    # 2. Load Data Sales
    t0 = time.time()
    sales_data = get_monthly_sales_data(start_year, end_year)
    print(f"[2/5] Load sales data selesai ({sales_data.shape[0]:,} baris) dalam {time.time() - t0:.2f} detik")

    # 3. Menggabungkan dan Menghitung (Merge & Calculate)
    t0 = time.time()
    final_data = merge_and_calculate(sales_data, stock_data, start_year, end_year)
    print(f"[3/5] Merge & Calculate selesai ({final_data.shape[0]:,} baris) dalam {time.time() - t0:.2f} detik")

    # 4. Merge Detail & Penataan Kolom
    t0 = time.time()
    finale = merge_detail(final_data, stock_data, start_year, end_year)
    finale = finale.loc[:, ~finale.columns.duplicated()]
    finale = ensure_stock_columns(finale)
    finale = finale.drop(
        columns=[c for c in finale.columns if c.lower().startswith('ts_ ')],
        errors='ignore'
    )
    print(f"[4/5] Merge Detail selesai ({finale.shape[0]:,} baris) dalam {time.time() - t0:.2f} detik")

    # 5. Simpan ke Database & Buat Index
    t0 = time.time()
    nama_tabel = f'sales_thru_pb_{str(start_year)[-2:]}'
    save_to_sql(finale, nama_tabel, 'imp')
    add_index_and_foreign_keys(
        'imp',
        nama_tabel,
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
    print(f"[5/5] Save to SQL & Indexing selesai dalam {time.time() - t0:.2f} detik")

    total_elapsed = time.time() - t_start
    timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n[INFO] Script stock_and_sales berhasil diselesaikan pada {timestamp_end} (Total waktu: {total_elapsed:.2f} detik).")
def md_status_artikel():
    df_artsq = df__artsizecode(col='fv_artsizecode, fv_barcode, fv_sizename, fv_configname', stat='imp')
    df = df__sales_thru_26('fv_artsizecode,fn_whconsiid,fn_stock_at') 
    df=df.merge(df_artsq,on='fv_artsizecode',how='left')
    df = proc_pivot_table_n(df,
                values='fn_stock_at',
                col_index=['fv_barcode','fn_whconsiid'],
                columns=['fv_sizename'],
                col_ket=['fv_configname'])
    df=df[['fv_barcode','fn_whconsiid','status']].drop_duplicates()
    
    df.to_excel('/opt/share/For MD/status_artikel.xlsx', index=False)

def md_artikel_size():
    df_cus= df__cus()
    # SELECT fn_sizeid, fv_artsizecode, fn_artsizeid from gis_db1.articlesize_tb
    df_artsq= df__artsizecode(col='fv_artsizecode, fv_barcode, fv_sizename,fv_configname',stat='imp')
    # print(df_artsq.columns)

    # df_sqq24= df__Sales_thru_24('*')
    df_sq25= df__sales_thru_25('*')
   
    # df_sq24=df_sqq24.copy()
    # df_sq25=df_sqq25.copy()

    # df_sq24 = pd.merge(df_sq24, df_artsq, on='fv_artsizecode', how='left')
    # df_sq24 = pd.merge(df_sq24,df_cus,on='fn_whconsiid',how='left')
    
    df_sq25 = pd.merge(df_sq25, df_artsq, on='fv_artsizecode', how='left', copy=False)
    df_sq25.info(memory_usage='deep')
    df_sq25 = pd.merge(df_sq25,df_cus,on='fn_whconsiid',how='left', copy=False)
    df_sq25.info(memory_usage='deep')
    # df_sq24['fn_artsizeid']=df_sq24['fn_artsizeid'].astype(int)
    df_sq25['fn_artsizeid'] = pd.to_numeric(df_sq25['fn_artsizeid'], downcast='integer')
    df_sq25.info(memory_usage='deep')

    # df_sq24['pk_id']=df_sq24['fn_whconsiid'].astype(str)+"/"+df_sq24['fn_artsizeid'].astype(str)
    df_sq25['pk_id']=df_sq25['fn_whconsiid'].astype(str)+"/"+df_sq25['fn_artsizeid'].astype(str)
    df_sq25=df_sq25.drop(columns=['fn_cusid'])
    #############
    # print(df_sq25.columns)  
    # df_sq24 = generate_rename_dict(2024, 2024, df_sq24)
    df_sq25 = generate_rename_dict(2025, 2025, df_sq25)
    # kolom_baru24 = (['pk_id','toko','code toko','artikel','size'] + 
    #                 [col for col in df_sq24.columns if col not in 
    #                 ['pk_id','toko','code toko','artikel', 'size','barcode','fv_artsizecode','fn_artsizeid','fn_whconsiid']])
    
    kolom_baru25 = (['pk_id','toko','code toko','artikel','size'] + 
                    [col for col in df_sq25.columns if col not in 
                    ['pk_id','toko','code toko','artikel', 'size','barcode','fv_artsizecode','fn_artsizeid','fn_sizeid','fn_whconsiid']])

    
    # df_sq24 = df_sq24[kolom_baru24]
    df_sq25 = df_sq25[kolom_baru25]
    del df_artsq, df_cus
    gc.collect()
    # df_sq24 = df_sq24.loc[~(df_sq24['artikel'].isnull())]
    print("Before pivot:")
    print(df_sq25.info(memory_usage='deep'))
    print(df_sq25.memory_usage(deep=True).sum() / 1024 / 1024, "MB")
    df_sq25 = df_sq25.loc[~(df_sq25['artikel'].isnull())]
    df = proc_pivot_table_n(df_sq25,
                    values='stock akhir 2025',
                    col_index=['code toko','artikel'],
                    columns=['size'],
                    col_ket=['fv_configname'],col_select=['code toko','artikel','status'])
    print("After pivot:")
    print(df.info(memory_usage='deep'))
    print(df.memory_usage(deep=True).sum() / 1024 / 1024, "MB")
    df_sq25=pd.merge(df_sq25,df[['code toko','artikel','status']],how='left',on=['code toko','artikel'])
    del df
    gc.collect()
    df_sq25=df_sq25.rename(columns={'fv_configname':'bot/top'})
    # df_result24=df_sq24.select_dtypes(include='number').sum()
    

    # df_sq24.to_excel('/opt/share/For MDstock artikel all store 2024.xlsx', index=False)
    total_rows = len(df_sq25)
    chunks = 3
    chunk_size = math.ceil(total_rows / chunks)  # otomatis bagi 3

    with pd.ExcelWriter('/opt/share/For MD/stock artikel all store 2025.xlsx', engine='xlsxwriter') as writer:
        for i in range(chunks):
            start_row = i * chunk_size
            end_row = min(start_row + chunk_size, total_rows)
            sheet_df = df_sq25.iloc[start_row:end_row]
            sheet_df.to_excel(writer, index=False, sheet_name=f'data_{i+1}')
    # df_result25=df_sq25.select_dtypes(include='number').sum()
    # # df_result24.to_excel('df_result24.xlsx')
    # df_result25.to_excel('/opt/share/PB DB/df_result25.xlsx')

def md_artikel_size_26():
    # 1. Ambil data pendukung
    df_cus = df__cus()
    df_artsq = df__artsizecode(col='fv_artsizecode, fv_barcode, fv_sizename, fv_configname', stat='imp')

    # OPTIMASI 1: Sangat disarankan ganti '*' dengan kolom yang benar-benar dipakai di SQL query level
    df_sq25 = df__sales_thru_26('*') 
    
    # 2. Downcast ke 'category' untuk hemat memori saat Join
    cat_cols = ['fv_artsizecode', 'fn_whconsiid']
    for col in cat_cols:
        if col in df_sq25.columns:
            df_sq25[col] = df_sq25[col].astype('category')
            
    for col in ['fv_artsizecode', 'fv_barcode', 'fv_sizename', 'fv_configname']:
        if col in df_artsq.columns:
            df_artsq[col] = df_artsq[col].astype('category')
    
    # 3. Merge Dataframe
    df_sq25 = pd.merge(df_sq25, df_artsq, on='fv_artsizecode', how='left', copy=False)
    del df_artsq
    gc.collect()
    
    df_sq25 = pd.merge(df_sq25, df_cus, on='fn_whconsiid', how='left', copy=False)
    del df_cus
    gc.collect()
    # Downcast ID integer
    if 'fn_artsizeid' in df_sq25.columns:
        df_sq25['fn_artsizeid'] = pd.to_numeric(df_sq25['fn_artsizeid'], downcast='integer')
    
    # OPTIMASI 2: Buat pk_id tanpa astype(str) yang membebankan objek Python
    df_sq25['pk_id'] = (
        df_sq25['fn_whconsiid'].astype(str) + '/' + df_sq25['fn_artsizeid'].astype(str)
    ).astype('category')

    if 'fn_cusid' in df_sq25.columns:
        df_sq25.drop(columns=['fn_cusid'], inplace=True)

    # 4. Rename & Reorder Kolom
    base_path = fr'/opt/share/for MD/'
    # base_path = fr"\\192.168.1.254\Share\_From Valdi\for MD"
    excel_path=fr'{base_path}status_artikel.xlsx'
    df_excel = pd.read_excel(excel_path)
    df_excel['status']=df_excel['status'].replace({"SEHAT":"OK"})
    # Merge status kembali ke dataframe utama (Garansi 1-to-1 match, tidak ada duplikasi)
    df_sq25 = pd.merge(
        df_sq25, 
        df_excel, 
        on=['fn_whconsiid', 'fv_barcode'], 
        how='left', 
        copy=False
    )
    
    del df_excel
    gc.collect()
    df_sq25 = generate_rename_dict(2026, 2026, df_sq25)
    # -------------------------------------------------------------------------
    # OPTIMASI 4: PENGGANTI PIVOT TABLE DENGAN AGREGASI / GROUPBY LIGHTWEIGHT
    # -------------------------------------------------------------------------
    # Jika logika 'status' ditentukan berdasarkan total stok atau atribut per (code toko, artikel),
    # kita hitung ringkasannya tanpa perlu membuat matriks pivot raksasa.
    
    # Menghitung total stok akhir per pasangan (code toko, artikel)
    # print(df_sq25.info())

    # df2=pd.merge(df2,df[['artikel','status']],how='left',on=['artikel'])
    # Drop kolom stok agregat karena hanya butuh status-nya saja
    kolom_baru25 = ['pk_id', 'toko', 'code toko', 'artikel', 'size'] + [
        col for col in df_sq25.columns if col not in 
        ['pk_id', 'toko', 'code toko', 'artikel', 'size', 'barcode', 'fv_artsizecode', 'fn_artsizeid', 'fn_sizeid', 'fn_whconsiid']
    ]
    df_sq25 = df_sq25[kolom_baru25]
    # OPTIMASI 3: Clean null & konversi teks ke category secepatnya
    df_sq25 = df_sq25.dropna(subset=['artikel'])
    text_cols_to_cat = ['toko', 'code toko', 'artikel', 'size', 'fv_configname']
    for col in text_cols_to_cat:
        if col in df_sq25.columns:
            df_sq25[col] = df_sq25[col].astype('category')

    print(f"Memory sebelum agregasi: {df_sq25.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB", flush=True)

    # Rename kolom fv_configname
    df_sq25.rename(columns={'fv_configname': 'bot/top'}, inplace=True)

    # -------------------------------------------------------------------------
    # OPTIMASI 5: Stream Writing ke Excel (Data Utuh + RAM Tetap < 100 MB)
    # -------------------------------------------------------------------------
    total_rows = len(df_sq25)
    chunks = 3
    chunk_size = math.ceil(total_rows / chunks)
    columns = list(df_sq25.columns)
    excel_path = '/opt/share/For MD/stock artikel all store 2026.xlsx'
    
    print(f"Mulai menulis {total_rows:,} baris ke Excel ({excel_path}) dalam {chunks} sheets...", flush=True)
    import xlsxwriter
    workbook = xlsxwriter.Workbook(excel_path, {'constant_memory': True, 'nan_inf_to_errors': True})
    for i in range(chunks):
        start_row = i * chunk_size
        end_row = min(start_row + chunk_size, total_rows)
        chunk_df = df_sq25.iloc[start_row:end_row]
        sheet_name = f'data_{i+1}'
        print(f"  -> Menulis sheet '{sheet_name}' (baris {start_row:,} s/d {end_row:,})...", flush=True)
        worksheet = workbook.add_worksheet(sheet_name)
        # 1. Tulis Header
        worksheet.write_row(0, 0, [str(c) for c in columns])
        # 2. Tulis Data Baris per Baris secara Sekuensial
        for r_idx, row in enumerate(chunk_df.itertuples(index=False, name=None), start=1):
            cleaned_row = [None if (v is None or pd.isna(v)) else v for v in row]
            worksheet.write_row(r_idx, 0, cleaned_row)
        del chunk_df
        gc.collect()
    workbook.close()
    del df_sq25
    gc.collect()
    print("✅ Selesai diproses ke Excel dengan aman tanpa OOM dan seluruh data terisi utuh!", flush=True)
    # df_result25=df_sq25.select_dtypes(include='number').sum()
    # # df_result24.to_excel('df_result24.xlsx')
    # df_result25.to_excel('/opt/share/PB DB/df_result25.xlsx')

import pandas as pd
from datetime import datetime


def artikel_size_value_26():
    """
    Generate laporan MD Artikel Size 2026 dengan Sales Value bulanan:
    - Tanpa kolom stok awal (hanya total stock dan qty per bulan).
    - Menambahkan kolom value penjualan (val {bulan} 26) di sebelah qty jual setiap bulan.
    - Menghitung total sales val 2026.
    - Menggabungkan status artikel dari status_artikel.xlsx.
    - Ekspor streaming sekuensial multi-sheet (3 sheets) menggunakan constant_memory:
      stock artikel all store 2026 value.xlsx
    """
    import time
    t_start = time.time()
    print("[INFO] Memulai proses artikel_size_value_26...", flush=True)

    # 1. Load Data Master & Sales Thru 2026
    df_cus = df__cus()
    df_artsq = df__artsizecode(col='fv_artsizecode, fv_barcode, fv_sizename, fv_configname', stat='imp')
    df_sq = df__sales_thru_26('*')

    # Downcast kategori untuk hemat memori saat join
    for col in ['fv_artsizecode', 'fn_whconsiid']:
        if col in df_sq.columns:
            df_sq[col] = df_sq[col].astype('category')
    for col in ['fv_artsizecode', 'fv_barcode', 'fv_sizename', 'fv_configname']:
        if col in df_artsq.columns:
            df_artsq[col] = df_artsq[col].astype('category')

    df_sq = pd.merge(df_sq, df_artsq, on='fv_artsizecode', how='left', copy=False)
    del df_artsq
    gc.collect()

    df_sq = pd.merge(df_sq, df_cus, on='fn_whconsiid', how='left', copy=False)
    del df_cus
    gc.collect()

    if 'fn_artsizeid' in df_sq.columns:
        df_sq['fn_artsizeid'] = pd.to_numeric(df_sq['fn_artsizeid'], downcast='integer')

    df_sq['pk_id'] = (
        df_sq['fn_whconsiid'].astype(str) + '/' + df_sq['fn_artsizeid'].astype(str)
    ).astype('category')

    if 'fn_cusid' in df_sq.columns:
        df_sq.drop(columns=['fn_cusid'], inplace=True)

    # 2. Monthly Sales Value dari gis_local.pb_penjualan_2026
    print("[INFO] Mengambil data value penjualan dari gis_local.pb_penjualan_2026...", flush=True)
    with get_engine('imp') as engine:
        query_val = """
        SELECT fn_whconsiid, fv_artsizecode, fn_month, 
               SUM(fn_totalpenjualan) AS val_sales
        FROM gis_local.pb_penjualan_2026
        GROUP BY fn_whconsiid, fv_artsizecode, fn_month
        """
        df_val = pd.read_sql(query_val, engine)

    months_abbr = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    year_short = '26'
    df_pivot_val = df_val.pivot(index=['fn_whconsiid', 'fv_artsizecode'], columns='fn_month', values='val_sales').fillna(0)
    val_cols = [f'val {months_abbr[m-1]} {year_short}' for m in df_pivot_val.columns]
    df_pivot_val.columns = val_cols
    df_pivot_val = df_pivot_val.reset_index()
    del df_val
    gc.collect()

    # 3. Merge Value & Status Artikel
    df_sq['fv_artsizecode'] = df_sq['fv_artsizecode'].astype(str)
    df_sq['fn_whconsiid'] = df_sq['fn_whconsiid'].astype('int64')
    df_pivot_val['fv_artsizecode'] = df_pivot_val['fv_artsizecode'].astype(str)
    df_pivot_val['fn_whconsiid'] = df_pivot_val['fn_whconsiid'].astype('int64')

    df_sq = pd.merge(df_sq, df_pivot_val, on=['fn_whconsiid', 'fv_artsizecode'], how='left', copy=False)
    del df_pivot_val
    gc.collect()

    for col in val_cols:
        if col in df_sq.columns:
            df_sq[col] = df_sq[col].fillna(0)
    df_sq['total sales val 2026'] = df_sq[val_cols].sum(axis=1)

    # Status artikel
    try:
        status_path = convert_to_local_path('/opt/share/For MD/status_artikel.xlsx')
    except Exception:
        status_path = '/opt/share/For MD/status_artikel.xlsx'

    if not os.path.exists(status_path):
        alt_status = convert_to_local_path('/opt/share/for MD/status_artikel.xlsx')
        if os.path.exists(alt_status):
            status_path = alt_status

    if os.path.exists(status_path):
        df_status = pd.read_excel(status_path)
        df_status['status'] = df_status['status'].replace({"SEHAT": "OK"})
        df_sq = pd.merge(df_sq, df_status, on=['fn_whconsiid', 'fv_barcode'], how='left', copy=False)
        del df_status
        gc.collect()
    else:
        print(f"⚠️ {status_path} tidak ditemukan, status di-set 'OK'", flush=True)
        df_sq['status'] = 'OK'

    # 4. Rename & Reorder Kolom (Hapus Stok Awal & Sisipkan Val di sebelah Qty)
    df_sq = generate_rename_dict(2026, 2026, df_sq)

    ordered_cols = ['pk_id', 'toko', 'code toko', 'artikel', 'size']
    for m in months_abbr:
        ts_col = f'total stock {m} {year_short}'
        qty_col = f'qty {m} {year_short}'
        val_col = f'val {m} {year_short}'
        if ts_col in df_sq.columns:
            ordered_cols.append(ts_col)
        if qty_col in df_sq.columns:
            ordered_cols.append(qty_col)
        if val_col in df_sq.columns:
            ordered_cols.append(val_col)

    summary_cols = [
        'stock akhir 2026', 
        'qty retur 2026', 
        'total stock 2026', 
        'total sales qty 2026', 
        'total sales val 2026'
    ]
    for sc in summary_cols:
        if sc in df_sq.columns:
            ordered_cols.append(sc)

    if 'fv_configname' in df_sq.columns:
        df_sq.rename(columns={'fv_configname': 'bot/top'}, inplace=True)
    if 'bot/top' in df_sq.columns:
        ordered_cols.append('bot/top')
    if 'status' in df_sq.columns:
        ordered_cols.append('status')

    df_sq = df_sq[ordered_cols].dropna(subset=['artikel'])

    # 5. Export ke Excel Streaming (Multi-Sheet Chunks)
    try:
        output_excel_path = convert_to_local_path('/opt/share/For MD/stock artikel all store 2026 value.xlsx')
    except Exception:
        output_excel_path = '/opt/share/For MD/stock artikel all store 2026 value.xlsx'

    os.makedirs(os.path.dirname(output_excel_path), exist_ok=True)

    chunks = 3
    total_rows = len(df_sq)
    chunk_size = math.ceil(total_rows / chunks)
    columns = list(df_sq.columns)

    print(f"[INFO] Mulai menulis {total_rows:,} baris ke Excel ({output_excel_path}) dalam {chunks} sheets...", flush=True)
    import xlsxwriter
    workbook = xlsxwriter.Workbook(output_excel_path, {'constant_memory': True, 'nan_inf_to_errors': True})

    for i in range(chunks):
        start_row = i * chunk_size
        end_row = min(start_row + chunk_size, total_rows)
        sheet_df = df_sq.iloc[start_row:end_row]
        sheet_name = f'data_{i+1}'
        print(f"  -> Menulis sheet '{sheet_name}' (baris {start_row:,} s/d {end_row:,})...", flush=True)

        worksheet = workbook.add_worksheet(sheet_name)

        # 1. Tulis Header
        worksheet.write_row(0, 0, [str(c) for c in columns])

        # 2. Tulis Data Baris demi Baris secara Sekuensial
        for r_idx, row in enumerate(sheet_df.itertuples(index=False, name=None), start=1):
            cleaned_row = [None if (v is None or pd.isna(v)) else v for v in row]
            worksheet.write_row(r_idx, 0, cleaned_row)

        del sheet_df
        gc.collect()

    workbook.close()
    del df_sq
    gc.collect()
    print(f"✅ [INFO] Selesai artikel_size_value_26 dalam {time.time() - t_start:.2f} detik. File: {output_excel_path}", flush=True)
    return df_sq


def md_artikel_size2():
    # def md_artikel_size2():
    # 1. Load data - Pastikan fungsi ini hanya mengambil kolom yang diperlukan
    # Misal: df__sales_thru_24(['fn_artsizeid', 'fv_artsizecode', 'sales_qty', ...])
    
    df_ots=get_stock_transit()
    df_sq24 = df__sales_thru_24('',gb='',sum_by=True)

    df_sq25 = df__sales_thru_25('',gb='',sum_by=True)

    df_sq26 = df__sales_thru_26('',gb='',sum_by=True)


    # List untuk memproses dataframe secara looping agar tidak copy-paste code
    dfs_sales = [df_sq24, df_sq25, df_sq26]
    years = [2024, 2025, 2026]

    for i, df_temp in enumerate(dfs_sales):
        # Rename menggunakan fungsi yang Anda miliki
        dfs_sales[i] = generate_rename_dict(years[i], years[i], dfs_sales[i])

    # 2. Merge secara bertahap dan hapus dataframe lama dari memori
    df = pd.merge(dfs_sales[0], dfs_sales[1], how='outer', on=['fv_artsizecode'])
    # del dfs_sales[0], dfs_sales[1] 
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='float') 
        # atau jika pasti angka bulat:
        df[col] = df[col].fillna(0).astype('int32')
    df = pd.merge(df, dfs_sales[2], how='outer', on=['fv_artsizecode'])

    # Bersihkan memori list sales
    del dfs_sales 

    # 3. Filter Kolom Berdasarkan Waktu
    current_date = datetime.now()
    current_month = current_date.month
    current_year_short = str(current_date.year)[-2:]
    current_year_long = str(current_date.year)

    month_map = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}

    # List comprehension lebih cepat daripada looping manual append
    cols_to_drop = [
        col for col in df.columns 
        if len(col.split()) >= 3 and 
        col.lower().split()[1] in month_map and 
        col.lower().split()[2] == current_year_short and 
        month_map[col.lower().split()[1]] > current_month
    ]
    df.drop(columns=cols_to_drop, inplace=True)
    # 4. Aggregasi All Time
    def fast_all_time(df, prefix, new_col):
        target_cols = [c for c in df.columns if c.lower().startswith(prefix.lower()) and 'all time' not in c.lower()]
        df[new_col] = df[target_cols].sum(axis=1, min_count=1) # min_count=1 menjaga handling NaN
        return df
    df = fast_all_time(df, 'qty retur ', 'qty retur all time')
    df = fast_all_time(df, 'total sales qty ', 'total sales qty all time')
    # 5. Final Join dengan Master Data
    df_sg = df__gdmd()
    df_sg['fv_artsizecode'] = df_sg['fv_artsizecode'].str.upper()
    # Groupby sebelum merge dengan master untuk mengecilkan ukuran data
    df = df.groupby(['fv_artsizecode'], as_index=False).sum(numeric_only=True)
    df = pd.merge(df, df_sg, on='fv_artsizecode', how='outer')
    df_artsq = df__artsizecode(col='fv_artsizecode, fv_barcode, fv_sizename,fv_configname', stat='imp')
    df = pd.merge(df, df_artsq, on='fv_artsizecode', how='left')
    # Kalkulasi Stock
    df['total stock all time'] = df['total sales qty all time'] + df.get(f'stock akhir {current_year_long}', 0)
    # print(df.info())
    df=generate_rename_dict(2000, 2000, df)
    # Cleaning
    df2 = df.dropna(subset=['artikel']).fillna(0)
    df2 = df2[df2['artikel'] != 0]
    df2=pd.merge(df2,df_ots,how='left',on=['fv_artsizecode'])
    kolom_baru = ['artikel', 'size'] + [col for col in df2.columns if col not in ['artikel', 'size','fv_artsizecode','fn_artsizeid']]
    df2 = df2[kolom_baru]
    # Pilih kolom yang diperlukan saja sebelum export
    df2['stock gudang dan toko'] = df2.get(f'stock akhir {current_year_long}', 0) + df2.get('stock gudang', 0)
    df = proc_pivot_table_n(df2,
                values='stock gudang dan toko',
                col_index=['artikel'],
                columns=['size'],
                col_ket=['fv_configname'])
    df = df[['artikel','status']]
    df_toko = proc_pivot_table_n(df2,
                values=f'stock akhir {current_year_long}',
                col_index=['artikel'],
                columns=['size'],
                col_ket=['fv_configname'])
    df_toko = df_toko[['artikel','status']].rename(columns={'status':'status_toko'})
    df2=pd.merge(df2,df[['artikel','status']],how='left',on=['artikel'])
    df2=pd.merge(df2,df_toko[['artikel','status_toko']],how='left',on=['artikel'])
    df2=df2.rename(columns={'fv_configname':'bot/top'})
    df2[['status','status_toko']]=df2[['status','status_toko']].replace({"SEHAT":"OK"})
    df2r=df2.select_dtypes(include='number').sum()
    # Save ke Excel (Gunakan engine 'xlsxwriter' untuk efisiensi jika perlu)
    df2.to_excel('/opt/share/For MD/stock artikel all time.xlsx', index=False)
    df2r.to_excel('/opt/share/For MD/artsize_gd_result.xlsx')
    # df2[['artikel','status','status_toko']].drop_duplicates().to_excel('/opt/share/For MD/status_artikel.xlsx', index=False)

    # Save artsize_gd_result ke Parquet per tanggal untuk tracking harian
    try:
        result_dir = convert_to_local_path('/opt/share/result')
    except Exception:
        result_dir = '/opt/share/result'
    os.makedirs(result_dir, exist_ok=True)

    tgl_str = datetime.now().strftime('%y%m%d')
    df2r_df = df2r.reset_index()
    df2r_df.columns = ['kolom', 'total']
    df2r_df['tanggal'] = datetime.now().strftime('%Y-%m-%d')

    file_parquet_daily = os.path.join(result_dir, f'artsize_gd_result-{tgl_str}.parquet')
    file_parquet_latest = os.path.join(result_dir, 'artsize_gd_result.parquet')

    df2r_df.to_parquet(file_parquet_daily, engine='pyarrow', index=False)
    df2r_df.to_parquet(file_parquet_latest, engine='pyarrow', index=False)
    print(f"✅ Parquet result saved: {file_parquet_daily}")

    return df2

def stock_by_artikel():
    base_path = fr'/opt/share/for MD/'
    base_path_input = fr'/opt/share/for Alokator/'
    # base_path = fr"\\192.168.1.254\Share\_From Valdi\for MD"
    excel_path=fr'{base_path}stock artikel all store 2026.xlsx'
    all_sheets = pd.read_excel(excel_path, sheet_name=None)
    # Menggabungkan semua DataFrame dalam dictionary menjadi 1 DataFrame
    df_excel = pd.concat(all_sheets.values(), ignore_index=True)
    df_excel=df_excel.drop(columns={'pk_id','size'})
    kolom_group=['toko','code toko','artikel','bot/top','status']
    kolom_numerik = [col for col in df_excel.columns if col not in kolom_group]
    df_grouped = df_excel.groupby(kolom_group, as_index=False,)[kolom_numerik].sum()
    df_grouped.to_excel(r"/opt/share/for Alokator/stock by artikel all store 2026.xlsx",index=False)

def sales_qty_value(year):
    df=df__pb_penjualan_(year)
    # Pivot keduanya sekaligus
    df_pivot = df.pivot_table(
        index=['fn_whconsiid','fv_artsizecode'],
        columns='fc_monthname',
        values=['qty','value'],
        aggfunc='sum',
        fill_value=0
    )

    # Flatten multiindex columns -> ubah jadi satu level
    df_pivot.columns = [f"{month}_{metric}" for metric, month in df_pivot.columns]

    # Reset index biar jadi kolom biasa
    df_final = df_pivot.reset_index()
def stock_daily():
    trx_rt()
def sales_daily(year):
    start_time2= datetime.now().strftime('%Y-%m-%d %H:%M:%S')    
    print("\n[INFO] Script sales_to_sql mulai dijalankan.")
    print(f"[INFO] Timestamp: {start_time2}\n")

    df1= df__penju_today()
    df= df1.copy()
    df= clean_and_transform_data(df)
    df=insert_disc2_grouping(df,'sum')
    # margin_tb(year)
    df=insert_margin(df,year)
    sum_col=['fn_jualpersize','total_penjualan2','total_disc2']
    agg_col=['fn_whconsiid', 'fv_artsizecode', 'tgltrx']
    fisrt_col=[col for col in df.columns if col not in sum_col + agg_col]
    col_col=fisrt_col+[col for col in df.columns if col not in fisrt_col]
    df=df[col_col]
    df['event']=df['event'].fillna(df['status_artikel'])
    df['fn_margin']=df['fn_margin'].fillna(0)
    df=rename_col_py_to_sql(df).sort_values(by='fn_idurut')
    df=insert_pk(df)
    df['fn_nett_amount'] = (df['fn_totalpenjualan']-df['fn_total_disc2']) * (1-df['fn_margin']/100)
    df['fn_sim_amount'] = np.where(df['fc_status'] == "X", df['fn_totalpenjualan'], df['fn_nett_amount'])
    df['fn_dpp'] = df['fn_nett_amount']/1.11
    df['fn_dpp2'] = df['fn_sim_amount']/1.11

    df_art=df_artikel_pb_('fv_artsizecode, fv_catname',distinct=False,where='')
    df=df.merge(df_art,how='left',on='fv_artsizecode')
    df=df.rename(columns={'fn_hargasatuan':'fm_amount','fv_status_artikel':'fv_status'})
    df['fv_ornal'] = df.apply(add_ornal, axis=1)
    df=df.rename(columns={'fm_amount':'fn_hargasatuan','fv_status':'fv_status_artikel'}).drop(columns='fv_catname')
    
    table_name = f"pb_penjualan_today"
    save_to_sql(df, table_name, 'imp')
    add_index_and_foreign_keys(
        'imp',
        table_name,
        index_cols=["fn_whconsiid", "fv_artsizecode"],
        foreign_keys=[
            {
                "column": "fn_whconsiid",
                "ref_table": "customers_pb",
                "ref_column": "fn_whconsiid",
                "constraint_name": f"fk_whconsiid_pb_sales_today"
            },
            {
                "column": "fv_artsizecode",
                "ref_table": "artikel_tb",
                "ref_column": "fv_artsizecode",
                "constraint_name": f"fk_sales_artsize_pb_today"
            }
        ]
        
    )
def stock_otsdo():
    df= df__otsdo()
def sales_consinetto():
    df_si=df__sales_invoice()
    df_sidisctok=sls_disctok()
    df_sii=pd.merge(df_si,df_sidisctok,how="left",on='fc_nodoc').fillna(0)
    df_sii['fin_dpp']=df_sii['dpp']-df_sii['disctok']
    df_sii['total_amount']=df_sii['fin_dpp']*df_sii['fn_qty']
    df_sii=rename_col_py_to_sql(df_sii).sort_values(by='fv_nodoc')
    nama_tabel='pb_penjualanpts'
    save_to_sql(df_sii, nama_tabel, 'imp')
    return 

def sales_bazar():
    df_bz_disc2=df__sales_bz_disc2nd()
    print(df_bz_disc2.info())
    df_bz=df__sales_bz()
    df_bz=pd.merge(df_bz,df_bz_disc2,how="left",on='fv_nopenjualan').fillna(0)
    print(df_bz.info())
    df_bz=clean_and_transform_data(df_bz)
    print(df_bz.info())
    df_bz=rename_col_py_to_sql(df_bz).sort_values(by='fn_idurut')
    nama_tabel='pb_penjualanbz'
    save_to_sql(df_bz, nama_tabel, 'imp')
    return 

def sales_ptsbz():
    df_si=df__sales_ptsbz()
    nama_tabel='penjualanbzrpts_pb'
    save_to_sql(df_si, nama_tabel, 'imp')
    return 

def sales_mp():
    df_si=df__sales_pmse()
    nama_tabel='penjualan_MrkPic_pb'
    save_to_sql(df_si, nama_tabel, 'imp')
    return 

def count_artikel():
    input_value = datetime.today().strftime("%Y-%m-%d")
    sales_thru = f"sales_thru_pb_{input_value[2:4]}"
    start_count = 1
    end_count= int(input_value[5:7])
    # stock = f"fn_stock_{start_count-end_count}"
    stocks = []  # list penampung
    for i in range(start_count, end_count + 1):
        stock = f"fn_stock_{i:02d}"
        stocks.append(stock)
    stocks = [f"fn_stock_{i:02d}" for i in range(start_count, end_count + 1)]
    stock_cols_str = ", ".join(stocks)+", fn_stock_at"
    df=df__sales_thru_25_count_artikel(stock_cols_str,sales_thru)
    df=df.groupby(['fn_whconsiid', 'fv_barcode']).sum().reset_index()
    col_art=[]
    # Cek jika ada nilai > 1 di kolom stock
    for i in range(start_count, end_count +1):
        stock_col = f'fn_stock_{i:02d}'
        art_col = f'fn_art_{i:02d}'
        df[art_col] = (df[stock_col] > 1).astype(int)
        col_art.append(art_col)
    df['fn_art_at']= df['fn_stock_at'] > 1
    df=df[['fn_whconsiid', 'fv_barcode'] + col_art + ['fn_art_at']]
    nama_tabel='pb_count_artikel'
    save_to_sql(df, nama_tabel, 'imp')
    return

def stock_borken_aging():
    # --- LOGIKA SEBELUM LOOP (TETAP SAMA) ---
    df_pend = df__draft_pending(colom="a.fv_nodraft,fc_type,a.fc_status,b.fc_status,a.fn_whconsiid2,a.fn_whconsiid,fv_artsizecode,fn_on_book", a__fc_status="X", b__fc_status="X")

    df_rt = df_pend.loc[df_pend['fc_type']=="RT", ['fn_whconsiid','fv_artsizecode','fn_on_book']].rename(columns={"fn_on_book":"fn_qty"})
    df_mo = df_pend.loc[df_pend['fc_type']=="MO", ['fn_whconsiid','fv_artsizecode','fn_on_book']].rename(columns={"fn_on_book":"fn_qty"})
    df_mi = df_pend.loc[df_pend['fc_type']=="MO", ['fn_whconsiid2','fv_artsizecode','fn_on_book']].rename(columns={"fn_whconsiid2":"fn_whconsiid","fn_on_book":"fn_qty"})
    df_ob = df_pend.loc[df_pend['fc_type']=="OB", ['fn_whconsiid2','fv_artsizecode','fn_on_book']].rename(columns={"fn_whconsiid2":"fn_whconsiid","fn_on_book":"fn_qty"})

    df_cus = df__customers_pb_("fn_cusid,fn_whconsiid")
    df_ostdo = df__otsdo()[['fn_whconsiid','fv_artsizecode','fn_qty']].dropna(subset='fn_whconsiid')
    df_ostdm = df__otsdm()[['fn_whconsiid2','fv_artsizecode','fn_qty']].rename(columns={"fn_whconsiid2":"fn_whconsiid"})
    df_ostob = df__otsob()[['fn_cusid','fv_artsizecode','OstOB']].rename(columns={"OstOB":"fn_qty"})
    df_ostob = df_ostob.merge(df_cus, how='left', on='fn_cusid')[['fn_whconsiid','fv_artsizecode','fn_qty']]

    # 1. Ubah nilai fn_qty & tambah label asal
    df_rt['fn_qty'] = -df_rt['fn_qty']
    df_rt['source'] = 'draf retur'
    df_mo['fn_qty'] = -df_mo['fn_qty']
    df_mo['source'] = 'draf mutasi out'

    df_mi['source'] = 'draf mutasi out (in)'
    df_ob['source'] = 'draf ob'
    df_ostdo['source'] = 'outstanding do'
    df_ostdm['source'] = 'outstanding dm'
    df_ostob['source'] = 'outstanding ob'

    # 2. Combine & Groupby
    df_combined = pd.concat([df_rt, df_mo, df_mi, df_ob, df_ostdo, df_ostdm, df_ostob], ignore_index=True)
    df_result = df_combined.groupby(['fn_whconsiid', 'fv_artsizecode'], as_index=False)['fn_qty'].sum()

    # Export Detail Pendingan
    df_art = df__artikel_pb("fv_artsizecode,fv_barcode, fv_sizename, fv_configname")
    df_cus = df__customers_pb_('fn_whconsiid, concat(fc_code," - ", fv_toko," - ",fv_kota) Toko', distinct=False, where='')

    df_combined_export = df_art.merge(df_combined, how='right', on='fv_artsizecode')
    df_combined_export = df_cus.merge(df_combined_export, how='right', on='fn_whconsiid')

    excel_path = f'/opt/share/For Alokator/Detail Pendingan.xlsx'
    df_combined_export.to_excel(excel_path, index=False)

    # Olah df_result
    df_result = df_art.merge(df_result, how='right', on='fv_artsizecode')[
        ["fn_whconsiid","fv_artsizecode","fv_barcode","fv_sizename","fv_configname","fn_qty"]
    ].rename(columns={"fn_qty":"stocks"})

    # Persiapan dataset dasar
    df = df__stock_today_all_store()
    df_aging = df__aging()

    df_future = pd.concat([df, df_result], ignore_index=True)
    df_future = (
        df_future
        .groupby(["fn_whconsiid","fv_artsizecode","fv_barcode","fv_sizename","fv_configname"], as_index=False)['stocks']
        .sum()
    )

    # --- START LOOP PROSES ---

    all_sizes_bottom = ['25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '40', '42', '44', 'ALLSIZE']
    all_sizes_top = ['FS', 'XS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL']
    all_sizes = all_sizes_bottom + all_sizes_top

    # Buat daftar target DataFrame dan path output-nya
    tasks = [
        (df, f'/opt/share/For Alokator/Data broken aging.xlsx'),
        (df_future, f'/opt/share/For Alokator/Data broken aging future.xlsx')
    ]

    for current_df, output_file in tasks:
        # 1. Olah aging & pivot
        df_processed = proc_add_aging(current_df, df_aging)
        pivot_table = proc_pivot_table(df_processed)
        
        # 2. Join dengan data customer / toko
        df_cus = df__customers_pb_('fn_whconsiid, concat(fc_code," - ", fv_toko," - ",fv_kota) Toko', distinct=False, where='')
        pivot_table = pivot_table.merge(df_cus, how='left', on='fn_whconsiid')
        
        # 3. Hitung Aging Tahun
        pivot_table['tahun_float'] = pivot_table['aging'] / 365
        pivot_table['Aging Tahun'] = (pivot_table['tahun_float'] * 2).astype(int) / 2
        
        # 4. Ambil aging gudang berdasarkan artikel
        list_art = list_to_sql_in(pivot_table, 'fv_barcode')
        where_list_art = f'and fv_barcode in ({list_art})'
        
        df_art_info = df_artikel_pb_('fn_articleid, fv_barcode', distinct=True, where=f'{where_list_art}')
        df_aging_gd = df__aging_pb_('fn_articleid, fn_hari', where='')
        
        df_art_info = df_art_info.merge(df_aging_gd, how='left', on='fn_articleid').drop(columns='fn_articleid')
        pivot_table = pivot_table.merge(df_art_info, how='left', on='fv_barcode').drop_duplicates()
        
        # 5. Export ke Excel sesuai file path-nya masing-masing
        cols_to_export = ['Toko', 'fv_barcode', 'status', 'sumQty', 'aging', 'Aging Tahun', *all_sizes]
        
        pivot_table[cols_to_export].rename(columns={
            'fv_barcode': 'Artikel',
            'sumQty': 'Jumlah stock',
            'fn_hari': 'Aging Gudang'
        }).to_excel(output_file, index=False)
        
    # df=df__stock_today_all_store()
    # df_aging = df__aging()
    # df = proc_add_aging(df, df_aging)
    # pivot_table = proc_pivot_table(df)
    # df_cus=df__customers_pb_('fn_whconsiid, concat(fc_code," - ", fv_toko," - ",fv_kota)  Toko',distinct=False,where='')
    # pivot_table=pivot_table.merge(df_cus,how='left',on='fn_whconsiid')
    # pivot_table['tahun_float'] = pivot_table['aging'] / 365
    # pivot_table['Aging Tahun'] = (pivot_table['tahun_float'] * 2).astype(int) / 2
    # list_art=list_to_sql_in(pivot_table,'fv_barcode')
    # list_art=f'and fv_barcode in ({list_art})'
    # df_art=df_artikel_pb_('fn_articleid, fv_barcode',distinct=True,where=f'{list_art}')
    # df_aging_gd=df__aging_pb_('fn_articleid, fn_hari', where='')
    # df_art=df_art.merge(df_aging_gd, how='left', on='fn_articleid').drop(columns='fn_articleid')
    # pivot_table=pivot_table.merge(df_art,how='left',on='fv_barcode').drop_duplicates()
    # nama_file = f'/opt/share/For Alokator/Data broken aging.xlsx'
    # all_sizes_bottom = ['25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38',
    #                     '40', '42', '44', 'ALLSIZE']
    # all_sizes_top = ['FS', 'XS',  'S', 'M', 'L', 'XL', 'XXL', 'XXXL']
    # all_sizes = all_sizes_bottom + all_sizes_top
    # pivot_table[['Toko','fv_barcode','status','sumQty','aging','Aging Tahun',*all_sizes]].rename(columns={
    #                 'fv_barcode':'Artikel',
    #                 'sumQty':'Jumlah stock',
    #                 'fn_hari':'Aging Gudang'
    #             }).to_excel(f'{nama_file}',index=False)
def fstok_gudang():
    df_stock_wh=df__stock_wh()
    if df_stock_wh.empty: 
        df_stock_wh=pd.DataFrame(columns=['fv_whtypename','fn_qty','fv_artsizecode'])
    df_fstock_wh=df__fstock_wh()
    # artsize='1144325902863'
    # safe_print(df_fstock_wh,'fv_artsizecode',artsize)
    if df_fstock_wh.empty: 
        df_fstock_wh=pd.DataFrame(columns=['fv_whtypename','fn_qty','fv_artsizecode'])
    
    df_onbook=df__onbook_stock()
    df_combi=pd.concat([df_stock_wh, df_fstock_wh,df_onbook], ignore_index=True)
    df_combi['fn_qty'] = df_combi['fn_qty'].astype(float)
    # artsize = df_combi['fv_artsizecode'].tolist()
    # # ubah list size jadi format SQL
    # list_artsize = ",".join([f"'{x}'" for x in artsize])
    # if list_artsize:
    #     query = f'''
    #             SELECT fv_artsizecode,fv_barcode from artikel_pb
    #             where fv_artsizecode in ({list_artsize})
    #             '''
    #     # df_art = pd.read_sql(query, engine_imp)
    #     results = DatabaseConnector.execute_raw_sql(query)
    #     df_art = pd.DataFrame(results)
    # else: df_art = pd.DataFrame(columns=['fv_artsizecode','fv_barcode'])
    # df_combi=pd.merge(df_combi, df_art, on='fv_artsizecode', how='left')
    
    df_combi = df_combi.groupby(['fv_artsizecode','fv_whtypename'], as_index=False)['fn_qty'].sum()
    list_art=list_to_sql_in(df_combi,'fv_artsizecode')
    list_art=f'and fv_artsizecode in ({list_art})'
    df_artsize= df_artikel_pb_('fv_barcode,fn_articleid,fv_artsizecode,fv_sizename,fv_configname',distinct=True,where=f'{list_art}')
    df_combi=df_combi.merge(df_artsize, how='left', on='fv_artsizecode')
    df_piv=pd.pivot_table(df_combi,
            values='fn_qty',
            index=['fv_barcode','fv_whtypename'],
            columns='fv_sizename',
            aggfunc='sum',
            fill_value=0).reset_index()
    all_sizes_bottom = ['25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38',
                        '40', '42', '44', 'ALLSIZE']
    all_sizes_top = ['FS', 'XS',  'S', 'M', 'L', 'XL', 'XXL', 'XXXL']
    all_sizes = all_sizes_bottom + all_sizes_top
    df_piv['sumQty']=df_piv[all_sizes].sum( axis=1)
    # print(df_piv.info())
    df_art= df_artikel_pb_('distinct fv_barcode,fv_catname,fv_brandname,fv_divname,fv_colorname,fv_rating,' \
        'fv_configname,fv_namemark,fm_price,fm_amount,fv_fitting,fv_status, fv_ornal, motif')
    df_aging=df__aging_gudang_pb()
    df_aging=df_aging.merge(df_artsize[['fv_barcode','fn_articleid']].drop_duplicates(),on='fn_articleid',how='left')
    df_broken=proc_pivot_table_n(df_combi.loc[df_combi['fv_whtypename'].isin(['PUSAT','RETUR','KONSINETTO'])],
                values='fn_qty',
                col_index=['fv_barcode'],
                columns=['fv_sizename'],
                col_ket=['fv_configname'])
    df_piv3=df_piv.merge(df_art,how='left',on='fv_barcode')
    df_piv3=df_piv3.merge(df_aging[['fv_barcode','fn_hari']],how='left',on='fv_barcode')
    df_piv3=df_piv3.merge(df_broken[['fv_barcode','status']],how='left',on='fv_barcode')
    
    df_piv3=df_piv3[['fv_barcode','fv_whtypename','fv_catname','fv_brandname','fv_divname','fv_colorname','fv_namemark',
                    'fv_fitting','fv_status','fv_ornal','motif','fv_configname','fv_rating','fn_hari','status','fm_price','fm_amount',*all_sizes,'sumQty']
                    ].rename(columns={
                        'fv_barcode':'Artikel',
                        'fv_catname':'Kategori',
                        'fv_whtypename':'Gudang',
                        'fv_brandname':'Brand',
                        'fv_divname':'Divisi',
                        'fv_colorname':'Color',
                        'fv_namemark':'Acara',
                        'fv_fitting':'Fitting',
                        'fv_status':'Status Produksi',
                        'fv_ornal':'Status',
                        'motif':'Motif',
                        'fn_hari':'Aging gundang',
                        'status':'Status barang',
                        'fm_price':'Harga Gross',
                        'fm_amount':'Harga Nett',
                        'sumQty':'Jumlah',
                        'fv_configname':'Top/Bottom',
                        'fv_rating':'Rating',
                    })
    nama_file = f'/opt/share/For Alokator/Freestock Gudang.xlsx'
    # df_combi[['fv_barcode','fn_qty']].rename(columns={
    #                 'fv_barcode':'Artikel',
    #                 'fn_qty':'Jumlah Freestock',
                    
    #             }).to_excel(f'{nama_file}',index=False)
    df_piv3.to_excel(f'{nama_file}',index=False)
    return

def aging_gudang_pb():
    query_gr = '''
        SELECT a.fc_nodoc, DATE(fd_arrivaldate) AS fd_arrivaldate, fv_nopo, fn_articleid, fn_qty FROM grdtl_tb a
        inner join grmst_tb b on a.fc_nodoc = b.fc_nodoc
        UNION ALL 
        SELECT a.fc_nodoc, DATE(a.fd_dateinput) fd_arrivaldate,a.fc_nodoc  fv_nopo, fn_articleid, fn_qty
        FROM stockindtl_tb a 
        inner join stockinmst_tb b on a.fc_nodoc = b.fc_nodoc
        
        '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df_gr = pd.read_sql(query_gr, engine)
    # Cari tanggal dengan qty tertinggi per store & product
    result = df_gr.sort_values('fn_qty', ascending=False).groupby(['fv_nopo', 'fn_articleid']).first().reset_index()

    # Urutkan dari tanggal terbaru
    df_sorted = result.sort_values('fd_arrivaldate', ascending=False)

    # Ambil baris pertama (arrival terbaru) untuk tiap articleid
    df_latest = df_sorted.drop_duplicates(subset='fn_articleid', keep='first')

    query_gr = '''
        SELECT distinct fn_articleid, fv_barcode FROM gis_local.artikel_pb 
        '''
    with get_engine('imp') as engine:  # Menggunakan context manager dengan with
        df_art = pd.read_sql(query_gr, engine)

    # unique_ids = pd.DataFrame(df_art['fn_articleid'].unique(), columns=[['fn_articleid','fv_barcode']])
    df_latest = pd.merge(df_latest, df_art, on='fn_articleid', how='right')

    df_latest=df_latest.fillna({'fn_qty':0, 'fd_arrivaldate':'2023-01-01', 'fv_nopo':'', 'fc_nodoc':''})
    df_latest["fd_arrivaldate"] = pd.to_datetime(df_latest["fd_arrivaldate"])

    today = pd.Timestamp(datetime.now().date())
    df_latest["fn_hari"] = (today - df_latest["fd_arrivaldate"]).dt.days
    df_latest["fn_bulan"] = (today.year - df_latest["fd_arrivaldate"].dt.year) * 12 + (today.month - df_latest["fd_arrivaldate"].dt.month)

    query = '''
    SELECT distinct fn_articleid, a.fd_dateinput FROM dodtl_tb a
    left join domst_tb b on a.fc_nodoc=b.fc_nodoc 
    where a.fc_nodoc like "DO%%"'''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df_fdo = pd.read_sql(query, engine)
    df_unique = (
        df_fdo
        .sort_values('fd_dateinput')
        .drop_duplicates(subset='fn_articleid', keep='first')
    )
    # df_latest.to_sql(f'aging_gudang_pb', engine_imp, if_exists='replace', index=False)
    df_latest = pd.merge(df_latest, df_unique, on='fn_articleid', how='left')
    df_latest["fn_hari_fdo"] = (today - df_latest["fd_dateinput"]).dt.days
    df_latest["fn_bulan_fdo"] = (today.year - df_latest["fd_dateinput"].dt.year
                            ) * 12 + (today.month - df_latest["fd_dateinput"].dt.month)
    df_latest = df_latest.assign(
        fd_dateinput = df_latest['fd_dateinput'].combine_first(df_latest['fd_arrivaldate']),
        fn_hari_fdo  = df_latest['fn_hari_fdo'].combine_first(df_latest['fn_hari']),
        fn_bulan_fdo = df_latest['fn_bulan_fdo'].combine_first(df_latest['fn_bulan'])
    )
    nama_tabel='aging_gudang_pb'
    save_to_sql(df_latest, nama_tabel, 'imp')
    return

def disc2nd_consi_online():
    df_disc2nd=df__sales_consi_disc2nd()
    df_disc3nd=df__sales_online_disc2nd()
    df_disc2nd=pd.concat([df_disc2nd, df_disc3nd])
    # Jumlahkan semua kolom kecuali 'qty', dan ambil 'qty' pertama
    agg_dict = {col: 'sum' for col in df_disc2nd.columns if col not in ['fv_nopenjualan', 'qty']}
    agg_dict['qty'] = 'first'
    # Grouping
    df_disc2nd = df_disc2nd.groupby('fv_nopenjualan', as_index=False).agg(agg_dict)
    # Calculate
    df_disc2nd['artsize_disc2nd'] = np.where(df_disc2nd['fn_potonganharga'] == 0,0,(df_disc2nd['fn_potonganharga'] / df_disc2nd['qty']).round(2))
    df_disc2nd['artsize_servisfee'] = np.where(df_disc2nd['servisfee'] == 0,0,(df_disc2nd['servisfee'] / df_disc2nd['qty']).round(2))
    df_disc2nd['artsize_premi'] = np.where(df_disc2nd['premi'] == 0,0,(df_disc2nd['premi'] / df_disc2nd['qty']).round(2))
    df_disc2nd['artsize_affiliate'] = np.where(df_disc2nd['affiliate'] == 0,0,(df_disc2nd['affiliate'] / df_disc2nd['qty']).round(2))
    df_disc2nd['artsize_voucher'] = np.where(df_disc2nd['voucher'] == 0,0,(df_disc2nd['voucher'] / df_disc2nd['qty']))
    df_disc2nd.replace([np.inf, -np.inf], 0, inplace=True)
    df_disc2nd = df_disc2nd.fillna(0)

    nama_tabel='disc2artsize_pb'
    save_to_sql(df_disc2nd, nama_tabel, 'imp')
    return  

def mini_consigment_sales(year):
    df=df__sales_(year,'fn_whconsiid,fv_artsizecode,fv_event, fd_tgltrx,fn_jualpersize,fn_totalpenjualan,fn_nett_amount,fv_nopenjualan,fn_tabel,fn_total_disc2')
    df_artpb=df__artikel_pb('fv_artsizecode,fv_barcode,fv_catname,fv_fitname,fv_brandname,fv_divname,fv_sizename,fm_price,fv_ornal,fv_status,fv_colorname,fv_fitting,motif,fv_namemark')
    df_cuspb=df__customers_pb('fv_toko,fc_code,fn_whconsiid,fc_detail_tok Group_toko')
    df=pd.merge(df, df_artpb, on='fv_artsizecode', how='left')
    df=pd.merge(df, df_cuspb, on='fn_whconsiid', how='left')
    print(df.info())
    df=df[['fd_tgltrx','fc_code','fv_toko','Group_toko','fv_barcode','fv_catname','fv_colorname',
           'fv_fitting','motif','fv_brandname','fv_nopenjualan','fv_divname','fv_sizename',
           'fv_event','fm_price','fv_ornal','fv_status','fn_totalpenjualan','fn_nett_amount',
           'fn_jualpersize','fv_namemark','fn_tabel','fn_total_disc2']]
    print(df.info())
    df=rename_col_sql_to_xlsx_gis(df)
    output_folder = f'/opt/share/For Alokator/Sales cosiment/{year}'
    df['Sales Date'] = pd.to_datetime(df['Sales Date'])
    # Tambahkan kolom bulan (format YYYY-MM)
    df['bulan_num'] = df['Sales Date'].dt.strftime('%m')
    df['bulan'] = df['Sales Date'].dt.strftime('%b')  # Nama bulan 3 huruf
    df['tahun'] = df['Sales Date'].dt.strftime('%y')  # Tahun dua digit
    df_tglimput=df__tgl_input_penju(year)
    print(df.info(),df_tglimput.info())
    df=df.merge(df_tglimput, how='left',on='fv_nopenjualan')
    print(df.info())
    df[['No. Pj','Status Master']]=df[['fv_nopenjualan','fv_namemark']]
    conditions = [
        df['fn_tabel']==1,
        df['fn_tabel']==2,
        df['fn_tabel']==3,
        df['fn_tabel']==4,
        df['fn_tabel']==5,
        df['fn_tabel']==6,
        df['fn_tabel']==7,
        df['fn_tabel']==8,
    ]

    # 2. Tentukan hasil untuk setiap kondisi
    choices = [ 'Counter', 
                'Location 1', 
                'Location 2', 
                'Location 3', 
                'Location 4', 
                'Bazar', 
                'Borongan', 
                'Online']

    # 3. Eksekusi (default='RT' digunakan untuk data yang tidak memenuhi DO atau DM)
    df['Nama Table'] = np.select(conditions, choices, default='Counter')
    df=df.drop(columns=['fv_nopenjualan','fn_tabel'])

    df=df[['Sales Date','Code Store','Store','Group_toko','Code Articel','Kategori','Color',
           	'Fitting','Motif','Brand','Division','Size','Status Article','HET','Ornal',
        	'Status Produksi','Total Nett Kassa','Nett Amount','Qty','fv_namemark'	,
            'fd_tglinput','No. Pj','Status Master','Nama Table','Disc Nota','bulan_num', 'bulan', 'tahun'
]]

    # Simpan ke Excel per bulan
    for (bulan_num, bulan, tahun), group in df.groupby(['bulan_num', 'bulan', 'tahun']):
        nama_file = f"{output_folder}/Sale Consi {bulan_num} {bulan} {tahun}.xlsx"
        group.drop(columns=['bulan', 'tahun', 'bulan_num']).to_excel(nama_file, index=False)
        print(f"✅ File disimpan: {nama_file}") 
    # 1. Daftar kolom tanpa 'Size'
    kolom_tanpa_size = [
        'Sales Date', 'Code Store', 'Store', 'Group_toko', 'Code Articel', 'Kategori', 'Color',
        'Fitting', 'Motif', 'Brand', 'Division', 'Status Article', 'HET', 'Ornal',
        'Status Produksi', 'Total Nett Kassa', 'Nett Amount', 'Qty', 'fv_namemark',
        'fd_tglinput', 'No. Pj', 'Status Master', 'Nama Table', 'Disc Nota', 'bulan_num', 'bulan', 'tahun'
    ]
    # Pilih kolom yang diinginkan dari DataFrame awal
    df_no_size = df[kolom_tanpa_size].copy()
    # 2. Tentukan kolom numerik yang ingin dijumlahkan (sum)
    kolom_numerik = ['Total Nett Kassa', 'Nett Amount', 'Qty', 'Disc Nota']
    # 3. Tentukan kolom grouping (semua kolom selain kolom numerik)
    kolom_group = [col for col in df_no_size.columns if col not in kolom_numerik]
    # 4. Agregasi data (group by kolom non-numerik, lalu sum kolom numerik)
    df_grouped = df_no_size.groupby(kolom_group, as_index=False, dropna=False)[kolom_numerik].sum()
    # 5. Simpan ke Excel per bulan dengan nama file yang berbeda
    for (bulan_num, bulan, tahun), group in df_grouped.groupby(['bulan_num', 'bulan', 'tahun']):
        # Menambahkan '_Summary' pada nama file agar berbeda
        nama_file = f"{output_folder}/Sale Consi Summary {bulan_num} {bulan} {tahun}.xlsx"
        # Drop kolom pembantu sebelum disimpan
        group.drop(columns=['bulan', 'tahun', 'bulan_num']).to_excel(nama_file, index=False)
        print(f"✅ File summary disimpan: {nama_file}")
        
def ost_ob():
    df_cus=df__customers_pb_('fn_cusid,fc_code "Code Toko",fv_toko Toko',distinct=False,where='')
    df_art=df_artikel_pb_('fv_artsizecode, fv_barcode "Artikel", fv_sizename "Size"' ,distinct=False,where='')
    df_user = df__user('fn_userid,fv_username "User"',)
    df_ostob_=df__otsob()
    df_ostob_=df_art.merge(df_ostob_,how='right',on='fv_artsizecode').drop(columns='fv_artsizecode')
    df_ostob_=df_cus.merge(df_ostob_,how='right',on='fn_cusid').drop(columns='fn_cusid')
    df_ostob_=df_ostob_.merge(df_user,how='left',on='fn_userid').drop(columns='fn_userid')
    nama_file = fr"/opt/share/For Alokator/Out Standing OB.xlsx"
    df_ostob_.to_excel(f'{nama_file}',index=False)
    return

def ost_do():
    df_cus=df__customers_pb_('fn_cusid,fc_code "Code Toko",fv_toko Toko',distinct=False,where='')
    df_art=df_artikel_pb_('fv_artsizecode, fv_barcode "Artikel", fv_sizename "Size"' ,distinct=False,where='')
    df_user = df__user('fn_userid,fv_username "User"',)
    input_value = (date.today().replace(day=1) - relativedelta(months=1)).strftime("%Y-%m-%d")
    df_ostdo_=df__otsdo_rt(input_value).drop(columns='fc_status')
    df_ostdo_=df_art.merge(df_ostdo_,how='right',on='fv_artsizecode').drop(columns='fv_artsizecode')
    df_ostdo_=df_cus.merge(df_ostdo_,how='right',on='fn_cusid').drop(columns='fn_cusid')
    df_ostdo_=df_ostdo_.merge(df_user,how='left',on='fn_userid').drop(columns='fn_userid')
    nama_file = fr"/opt/share/For Alokator/Out Standing DO.xlsx"
    df_ostdo_.to_excel(f'{nama_file}',index=False)
    return

def penjualan_by_table(year):
    df=get_penjualan_tabel(year)
    return

def master_gudang():
    df=df__stock_wh()
    df_artsize= df_artikel_pb_('fv_barcode,fn_articleid,fv_artsizecode,fv_sizename,fv_configname')
    df=df.merge(df_artsize,how='left',on='fv_artsizecode')
    df_piv=pd.pivot_table(df,
            values='fn_qty',
            index=['fv_barcode','fv_whtypename'],
            columns='fv_sizename',
            aggfunc='sum',
            fill_value=0).reset_index()
    all_sizes_bottom = ['25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38',
                        '40', '42', '44', 'ALLSIZE']
    all_sizes_top = ['FS', 'XS',  'S', 'M', 'L', 'XL', 'XXL', 'XXXL']
    all_sizes = all_sizes_bottom + all_sizes_top
    df_piv['sumQty']=df_piv[all_sizes].sum( axis=1)
    # print(df_piv.info())
    df_art= df_artikel_pb_('distinct fv_barcode,fv_catname,fv_brandname,fv_divname,fv_colorname,fv_rating,' \
        'fv_configname,fv_namemark,fm_price,fm_amount,fv_fitting,fv_status, fv_ornal, motif')
    df_aging=df__aging_gudang_pb()
    print(df_aging.info())
    df_aging=df_aging.merge(df_artsize[['fv_barcode','fn_articleid']].drop_duplicates(),on='fn_articleid',how='left')
    
    print(df_aging.info())
    df_broken=proc_pivot_table_n(df.loc[df['fv_whtypename'].isin(['PUSAT','RETUR','KONSINETTO'])],
                values='fn_qty',
                col_index=['fv_barcode'],
                columns=['fv_sizename'],
                col_ket=['fv_configname'])
    df_piv3=df_piv.merge(df_art,how='left',on='fv_barcode')
    df_piv3=df_piv3.merge(df_aging[['fv_barcode','fn_hari','last receive gudang']],how='left',on='fv_barcode')
    df_piv3=df_piv3.merge(df_broken[['fv_barcode','status']],how='left',on='fv_barcode')
    
    print(df_piv3.info())
    df_piv3=df_piv3[['fv_barcode','fv_whtypename','fv_catname','fv_brandname','fv_divname','fv_colorname','fv_namemark',
                    'fv_fitting','fv_status','fv_ornal','motif','fv_configname','fv_rating','fn_hari','status','fm_price','fm_amount',*all_sizes,'sumQty'
                    ,'last receive gudang']
                    ].rename(columns={
                        'fv_barcode':'Artikel',
                        'fv_catname':'Kategori',
                        'fv_whtypename':'Gudang',
                        'fv_brandname':'Brand',
                        'fv_divname':'Divisi',
                        'fv_colorname':'Color',
                        'fv_namemark':'Acara',
                        'fv_fitting':'Fitting',
                        'fv_status':'Status Produksi',
                        'fv_ornal':'Status',
                        'motif':'Motif',
                        'fn_hari':'Aging gundang',
                        'status':'Status barang',
                        'fm_price':'Harga Gross',
                        'fm_amount':'Harga Nett',
                        'sumQty':'Jumlah',
                        'fv_configname':'Top/Bottom',
                        'fv_rating':'Rating',
                    })
    nama_file = fr"/opt/share/For Alokator/Data Master Gudang.xlsx"
    df_piv3.to_excel(f'{nama_file}',index=False)
    return
def upsert_master(): 
    df2=df__artikel_pb("*")
    df_aging=df__aging_gudang_pb()
    df2=df2.merge(df_aging,how='left',on='fn_articleid').rename(columns={
                        'fn_hari_fdo':'aging'})
    df2.to_excel('/opt/share/For Alokator/MASTER ARTIKEL.xlsx', index=False)

def upsert_artikel_pb():
    df= df__artikel_pb_read()
    # df['fv_artsizecode'] = df['fv_artsizecode'].str.replace(" ", "", regex=False)
    # df['fv_barcode'] = df['fv_barcode'].str.replace(" ", "", regex=False)
    df = df.drop_duplicates(subset=['fv_artsizecode'])
    # df_nfa= get_artikel_md()
    # df=pd.merge(df,df_nfa,on='fv_barcode',how='left')
    df=df.loc[~df['fv_artsizecode'].isnull()]
    # df2=df__artikel_pb("*")
    colist=df.columns.to_list()
    df['fv_neo_link_foto']='none'
    df[['fv_rating']]=df[['fv_rating']].fillna('none')
    # df2[['fv_rating','fv_neo_link_foto']]=df2[['fv_rating','fv_neo_link_foto']].fillna('none')
    df['fv_ornal'] = df.apply(add_ornal, axis=1)
    df3=df
    if df3.empty: return print("✅ Tidak Data yang diupsert!")
    df3=df3.fillna('none')
    df_art_status=df__artikel_status()
    df3=pd.merge(df3,df_art_status,on='fn_articleid',how='left')
    df3=df3[['fv_barcode',
    'fv_artsizecode',
    'fv_catname',
    'fv_brandname',
    'fv_divname',
    'fv_sizename',
    'fm_price',
    'fm_amount',
    'fv_namemark',
    'fv_colorname',
    'fv_fitname',
    'fv_tomname',
    'fv_configname',
    'motif',
    'fv_picture',
    'fv_rating',
    'fm_cogs',
    'fn_articleid',
    'fv_neo_link_foto',
    'fv_fitting',
    'fv_status',
    'fv_ornal',
    'fc_hold']]
    df3['fm_amount'] = pd.to_numeric(df3['fm_amount'], errors='coerce') 
    df3['fm_amount'] = df3['fm_amount'].fillna(0)
    df3['fm_price'] = pd.to_numeric(df3['fm_price'], errors='coerce')  # jadi NaN kalau gak valid
    df3['fm_price'] = df3['fm_price'].fillna(0)
    table_name = 'artikel_pb'
    for col in df3.columns:
        if col != 'fv_artsizecode':
            df_sub = df3[['fv_artsizecode', col]].copy()
            upsert_data('imp', table_name, df_sub, primary_key='fv_artsizecode')
            print(col)

def upsert_customers_pb():
    base_path = fr'/opt/share/PB DB/'
    base_path2 = fr'/opt/share/For Alokator/'
    excel_path = f'{base_path}customers_pb_sup.xlsx'
    csv_path = f'{base_path}list user & employee.csv'
    pic_path = f'{base_path}LIST PIC MARKETING.xlsx'
    mic_path = f'{base_path}PIC MIC.xlsx'
    mic_path2 = f'{base_path2}data toko.xlsx'
    table_name = 'customers_pb'
    df = df__customers_pb_read()
    # df["fn_whconsiid"] = df["fn_whconsiid"].fillna(0).astype("int64")

    # mask = df["fn_whconsiid"] == 0

    # start = df.loc[~mask, "fn_whconsiid"].max()
    # if pd.isna(start):
    #     start = 0

    # df.loc[mask, "fn_whconsiid"] = range(
    #     start + 1,
    #     start + 1 + mask.sum()
    # )
    df_excel = pd.read_excel(excel_path)
    emp_tb=pd.read_csv(csv_path)
    emp_tb['fn_userid']=emp_tb['fn_userid'].astype(int)
    df_user=df__cus_user()
    df_user=pd.merge(df_user,emp_tb[['fn_userid','fv_nameemployees']],on='fn_userid',how='left')
    df_user['fn_cusid']=df_user['fn_cusid'].astype(int)
    df=pd.merge(df,df_user[['fn_cusid','fv_nameemployees']],on='fn_cusid',how='left')
    df=df.loc[~df['fn_whconsiid'].duplicated()]
    df=pd.merge(df, df_excel, on='fn_cusid', how='left')

    df_picmkt=pd.read_excel(pic_path, header=3).dropna(how='all')
    df_picmkt['SUPERVISOR/ OJT**'] = df_picmkt['SUPERVISOR/ OJT**'].str.replace(r'[^A-Za-z\s]', '', regex=True).str.strip().str.title()
    df=pd.merge(df,df_picmkt[['MANAGER','SUPERVISOR/ OJT**','KODE TOKO GIS']],left_on='code',right_on='KODE TOKO GIS',how='left')
    df=df.rename(columns={'MANAGER_y':'MANAGER'})
    df['SUPERVISOR']=df['SUPERVISOR/ OJT**']

        # 1. Baca PIC MIC
    df_picmic = pd.read_excel(mic_path).dropna(how='all')
    picmic_cols = [c for c in ['PIC MIC', 'STORE GIS'] if c in df_picmic.columns]
    if 'STORE GIS' in picmic_cols:
        df = pd.merge(df, df_picmic[picmic_cols], left_on='code', right_on='STORE GIS', how='left').fillna('-')

    # 2. Baca data toko Alokator (mic_path2)
    df_alokator = pd.read_excel(mic_path2).dropna(how='all')
    
    # Normalisasi nama kolom: strip whitespace dan lowercase
    df_alokator.columns = df_alokator.columns.astype(str).str.strip().str.lower()

    # Mapping fleksibel jika nama kolom bervariasi
    rename_map = {}
    for col in df_alokator.columns:
        if col in ['kode toko', 'kode_toko', 'kodetoko', 'store_code', 'kode']:
            rename_map[col] = 'kode_toko'
        elif col in ['pic', 'pic alokator', 'alokator', 'nama pic']:
            rename_map[col] = 'pic'
    df_alokator = df_alokator.rename(columns=rename_map)

    # Merge jika kolom tersedia
    if 'kode_toko' in df_alokator.columns and 'pic' in df_alokator.columns:
        df = pd.merge(df, df_alokator[['kode_toko', 'pic']], left_on='code', right_on='kode_toko', how='left').fillna('-')
        df['Alokator'] = df['pic']
    else:
        print(f"⚠️ Kolom 'kode_toko' / 'pic' tidak ditemukan di {mic_path2}. Kolom terbaca: {df_alokator.columns.tolist()}")
        df['Alokator'] = '-'

    df.loc[df['fn_cusid'] == 915, 'fn_whconsiid'] = 9000
    
    # Gunakan errors='ignore' agar tidak KeyError jika ada kolom yang tidak terdaftar
    drop_cols = ['KODE TOKO GIS', 'MANAGER_x', 'SUPERVISOR/ OJT**', 'STORE GIS', 'PIC MIC', 'pic', 'kode_toko']
    df = df.drop(columns=drop_cols, errors='ignore')

    if df is None or df.empty:
        print("❌ DataFrame kosong! Upsert dibatalkan.")
    else:
        print("✅ Data berhasil di-load dengan kolom:", df.columns.tolist())
    df = process_data_cus(df)
    df = df.where(pd.notnull(df), None)
    
    upsert_data('imp',table_name,df, primary_key='customers_pb')

def upsert_tgl_so_sa():
    df=df__tglsa_()
    nama_tabel='tgl_sa'
    save_to_sql(df, nama_tabel, 'imp')
    df=df__tglso_()
    nama_tabel='tgl_so'
    save_to_sql(df, nama_tabel, 'imp')

def upsert_whtype():
    df=df__whtype_name()
    nama_tabel='whtype'
    save_to_sql(df, nama_tabel, 'imp')

def upsert_expedition_tracking():
    print('proses "get_nodo_tglreceive"')
    get_nodo_tglreceive('yes',2026)
    print('proses "df__date_arrival"')
    df_date_arv=df__date_arrival()
    # print(df_date_arv.info())
    print('proses "df__tracking_do"')
    df_do=df__tracking_do()
    print('proses "df__tracking_rm"')
    df_rm=df__tracking_rm()
    print('proses transform')
    df=pd.concat([df_do,df_rm],ignore_index=True)
    df=df.merge(df_date_arv, left_on='nodo',right_on='No. DO', how='left')
    df=df.rename(columns={'Date of Receipt':'date_arrival','Description':'nosi','No. Tracking Number':'nosi_add'})
    df['fn_cusid'] = df['fn_cusid'].fillna(0).astype(int)
    df['nodo']= df['nodo'].fillna(df['noob'])
    nama_tabel='expedition_tracking_pb'
    # print(df.info())
    df=df.rename(columns={'nosi':'fv_nosi','fv_nosi':'fv_nosi_sistem'})
    df['fn_whconsiid2']=df['fn_whconsiid2'].fillna(0).astype('int')

    # Kita gunakan .fillna('') agar data null tidak error saat dicek startswith
    conditions = [
        df['nodo'].fillna('').str.startswith('DO'),
        df['nodo'].fillna('').str.startswith('DM'),
        df['nodo'].isna()
    ]

    # 2. Tentukan hasil untuk setiap kondisi
    choices = ['DO', 'DM', 'RT']

    # 3. Eksekusi (default='RT' digunakan untuk data yang tidak memenuhi DO atau DM)
    df['fc_ket_sj'] = np.select(conditions, choices, default='RT')
    
    print('proses save sql ')
    save_to_sql(df, nama_tabel, 'imp')
    
    print('proses df detail')
    df_detail=df__detail_tracking(2026)

    nama_tabel='expedition_tracking_dtl_pb_2026'
    print('proses save sql detail')
    save_to_sql(df_detail, nama_tabel, 'imp')