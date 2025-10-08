import pandas as pd
import numpy as np
import gc
import math
from datetime import date, timedelta
import calendar
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy import Integer, String, CHAR, Date, Text, text, Float
import os
from datetime import datetime
from modules.queries import (df__penju, df__tokoawal, df__transaction, df__cus, get_monthly_sales_data, 
                             df__sales_thru_24, df__sales_thru_25, df__sales_thru_25_count_artikel, df__artsizecode, df__gdmd, fetch_data_margin,
                             df__sales_, df__customers_pb, df__artikel_pb, df__artikel_pb_read, 
                             df__customers_pb_read, df__cus_user, df__sales_invoice,  df__sales_bz_disc2nd,
                             df__sales_bz, df__sales_consi_disc2nd, df__sales_online_disc2nd, df__otsdm, df__otsdo,
                             df__penju_today, df__artikel_status, df__tglso_, df__tglsa_, df__whtype_name, df__aging_gudang ) 
from modules.proses import (save_to_sql, add_index_and_foreign_keys, clean_and_transform_data, 
                            insert_disc2_grouping, insert_margin, rename_col_py_to_sql, insert_pk,
                            get_tgl, get_stock, get_stock_data, merge_and_calculate, merge_detail, 
                            generate_rename_dict,trx_rt, process_data_margin,rename_col_sql_to_xlsx_gis,
                            get_artikel_md, get_new_or_changed_rows,upsert_data, process_data_cus,
                            sls_disctok, proc_pivot_table_n, add_ornal)
def margin_tb(year):
    df_trx, df_artimp, df_artred, df_atrxu, df_mrgincat, df_mrgin = fetch_data_margin(year)
    df_trx_final = process_data_margin(df_trx, df_artimp, df_artred, df_atrxu, df_mrgincat, df_mrgin)
    save_to_sql(df_trx_final, f'margin{str(year)[-2:]}_pb', 'imp')

def sales_to_sql(year):
    start_time2= datetime.now().strftime('%Y-%m-%d %H:%M:%S')    
    print("\n[INFO] Script sales_to_sql mulai dijalankan.")
    print(f"[INFO] Timestamp: {start_time2}\n")

    df1= df__penju(year)
    df= df1.copy()
    df= clean_and_transform_data(df)
    df=insert_disc2_grouping(df,'sum')
    margin_tb(year)
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
    expot_excel = expot_excel.lower()
    if expot_excel not in ['yes', 'no']:
        raise ValueError("Parameter 'expot_excel' harus berupa 'yes' atau 'no'.")
    tglnow = datetime.today().date() # Tanggal Perhari Ini 
    df_tokoawal = df__tokoawal()
    idwhtoko_list_awal = df_tokoawal['fn_whconsiid'].tolist() # Ubah kedalam List

    # Mencari tanggal SO terakhir utnuk masing-masing toko yang masih buka per tanggal yang dipilih
    df_tglLSO = pd.concat([get_tgl(idwhtoko, tglnow, tglEND) for idwhtoko in idwhtoko_list_awal])
    df_tglLSO['fd_tglflagIndo'] = pd.to_datetime(df_tglLSO['fd_tglflagIndo'])

    if tglEND2 > tglnow:  # Jika tanggal yang dipilih lebih besar dari hari ini
        df_tglLSO.loc[df_tglLSO['fc_status']=="T",'fd_tglflagIndo'] -= pd.Timedelta(days=1)

    df_tglLSO['fd_tglflagIndo'] = df_tglLSO['fd_tglflagIndo'].dt.strftime('%Y-%m-%d')
    idwhtoko_list = df_tglLSO['fn_whconsiid'].tolist() # Ubah kedalam List untuk kode toko
    tgl_list = df_tglLSO['fd_tglflagIndo'].tolist() # Ubah kedalam List untuk tanggal SO terakhir

    # Mencari Stock pertanggal yang dipilih dengan menjumlah stock awal SO terakhir dengan transaksi dari SO terakhir sampai tanggal yang dipilih
    df_tok = pd.DataFrame() # Dataframe Kosong
    for idwhtoko, tglSTR in zip(idwhtoko_list, tgl_list): # Looping untuk masing-masing toko dan tanggal SO terakhir
        df_stock = get_stock(idwhtoko, tglSTR, tglEND) # Mencari Stock awal
        df_stock['fv_artsizecode'] = df_stock['fv_artsizecode'].str.upper()
        df_stock['fv_artsizecode'] = df_stock['fv_artsizecode'].replace({'BBRD103V042333-': 'BBRD103V042333'})
        df_transactions = df__transaction(idwhtoko, tglSTR, tglEND) # Mencari semua transaksi
        df_transactions['fv_artsizecode'] = df_transactions['fv_artsizecode'].str.upper()
        df_transactions['fv_artsizecode'] = df_transactions['fv_artsizecode'].replace({'BBRD103V042333-': 'BBRD103V042333'})
        df_total = pd.concat([df_stock, df_transactions]).groupby('fv_artsizecode').sum().reset_index() # menjumlah stock awal dan transaksi
        df_total['fn_whconsiid'] = idwhtoko 
        df_total['tgltrx'] = tglEND
        
        df_tok = pd.concat([df_tok, df_total], ignore_index=True)
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

def get_last_three_months_including_current():
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
    for tgl in last_dates:
        get_stock_monthly(tgl, 'yes')
    # Print tanggal yang dipilih sebelum return
    print("[INFO] Tanggal yang dipilih:")
    print("\n".join(last_dates))

    return last_dates

def stock_and_sales(start_year, end_year):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("\n[INFO] Script stock_and_sales mulai dijalankan.")
    print(f"[INFO] Timestamp: {timestamp}\n")
    base_path = '/opt/share/PB DB/'
    print("Base path:", base_path)
    print(os.path.exists(base_path)) 

    stock_data_25 = get_stock_data(base_path, start_year, end_year)
    print(stock_data_25.info())
    print("stock_data prosess complite")

    sales_data_25 = get_monthly_sales_data(start_year, end_year)
    print(sales_data_25.columns)
    print("Sales_data prosess complite")
    
    # Menggabungkan dan menghitung
    final_data_25 = merge_and_calculate(sales_data_25, stock_data_25, start_year, end_year)
    print("final_data prosess complite")

    finale_25 = merge_detail(final_data_25, stock_data_25, start_year, end_year )
    tanggal_selesai = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    print(f"finale prosess complite {tanggal_selesai}")

    nama_tabel=f'sales_thru_pb_{str(start_year)[-2:]}'
    # finale_25.to_sql(nama_tabel, engine2, if_exists='replace', index=False, dtype=sql_types)
    save_to_sql(finale_25, nama_tabel, 'imp')
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
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("\n[INFO] Script telah di input di gis.local.")
    print(f"[INFO] Timestamp: {timestamp}")
        # Mendapatkan tanggal saat ini

    save_to_sql(finale_25, nama_tabel, 'read')
    # add_index_and_foreign_keys(nama_tabel, ['fn_whconsiid', 'fv_artsizecode'])
    
    tanggal_selesai = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        # Menambahkan pesan status berhasil
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("\n[INFO] Script telah berhasil dijalankan.")
    print(f"[INFO] Timestamp: {timestamp}")

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
                    values='stock akhir',
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

def md_artikel_size2():
    df_artsq= df__artsizecode(col='fv_artsizecode, fv_barcode, fv_sizename,fv_configname',stat='imp')
    df_sqq24= df__sales_thru_24('*')
    df_sqq25= df__sales_thru_25('*')
    df_sg= df__gdmd()

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
    # df['qty retur']=df['qty retur_x'].fillna(0)+df['qty retur_y'].fillna(0)
    # print(df.columns)
    df=df.rename(columns={
        'qty retur_x': 'retur 2024',
        'qty retur_y': 'retur 2025',})
    df['stock akhir']=df['stock akhir_y']
    df['total sales qty']=df['total sales qty_x'].fillna(0)+df['total sales qty_y'].fillna(0)
    df['total stock']=df['total sales qty']+df['stock akhir'].fillna(0)
    df=df.drop(columns=['total stock_x', 'stock akhir_x', 'stock akhir_y',
                        'total stock_y','total sales qty_x','total sales qty_y'])
    df_sg['fv_artsizecode']=df_sg['fv_artsizecode'].str.upper()
    df = df.groupby(['fv_artsizecode'], as_index=False).sum()
    df = pd.merge(df, df_sg, on='fv_artsizecode', how='outer')
    df = pd.merge(df, df_artsq, on='fv_artsizecode', how='left')
    df =generate_rename_dict(2025, 2025, df)
    df = df.loc[~(df['artikel'].isnull())]
    df2=df.fillna(0)
    df2=df2[df2['artikel']!=0]
    kolom_baru24 = ['artikel', 'size'] + [col for col in df2.columns if col not in ['artikel', 'size','fv_artsizecode','fn_artsizeid']]
    df2 = df2[kolom_baru24]
    df = proc_pivot_table_n(df2,
                    values='stock akhir',
                    col_index=['artikel'],
                    columns=['size'],
                    col_ket=['fv_configname'])
    df = df[['artikel','status']]
    df2=pd.merge(df2,df[['artikel','status']],how='left',on=['artikel'])
    df2=df2.rename(columns={'fv_configname':'bot/top'})
    df2r=df2.select_dtypes(include='number').sum()
    df2r.to_excel('/opt/share/For MD/artsize_gd_result.xlsx')
    df2.to_excel('/opt/share/For MD/stock artikel all 2025.xlsx', index=False)

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

def aging_gudang():
    df=df__aging_gudang()
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
    df=df__sales_(year,'fn_whconsiid,fv_artsizecode,fv_event, fd_tgltrx,fn_jualpersize,fn_totalpenjualan,fn_nett_amount')
    df_artpb=df__artikel_pb('fv_artsizecode,fv_barcode,fv_catname,fv_fitname,fv_brandname,fv_divname,fv_sizename,fm_price,fv_ornal,fv_status')
    df_cuspb=df__customers_pb('fv_toko,fc_code,fn_whconsiid')
    df=pd.merge(df, df_artpb, on='fv_artsizecode', how='left')
    df=pd.merge(df, df_cuspb, on='fn_whconsiid', how='left')
    df=df[['fd_tgltrx','fc_code','fv_toko','fv_barcode','fv_catname','fv_fitname','fv_brandname',
    'fv_divname','fv_sizename','fv_event','fm_price','fv_ornal','fv_status','fn_totalpenjualan','fn_nett_amount','fn_jualpersize']]
    df=rename_col_sql_to_xlsx_gis(df)
    output_folder = f'/opt/share/For Alokator/Sales cosiment/{year}'
    df['Sales Date'] = pd.to_datetime(df['Sales Date'])
    # Tambahkan kolom bulan (format YYYY-MM)
    df['bulan_num'] = df['Sales Date'].dt.strftime('%m')
    df['bulan'] = df['Sales Date'].dt.strftime('%b')  # Nama bulan 3 huruf
    df['tahun'] = df['Sales Date'].dt.strftime('%y')  # Tahun dua digit

    # Simpan ke Excel per bulan
    for (bulan_num, bulan, tahun), group in df.groupby(['bulan_num', 'bulan', 'tahun']):
        nama_file = f"{output_folder}/Sale Consi {bulan_num} {bulan} {tahun}.xlsx"
        group.drop(columns=['bulan', 'tahun', 'bulan_num']).to_excel(nama_file, index=False)
        print(f"✅ File disimpan: {nama_file}") 

def upsert_artikel_pb():
    df= df__artikel_pb_read()
    df['fv_artsizecode'] = df['fv_artsizecode'].str.replace(" ", "", regex=False)
    df = df.drop_duplicates(subset=['fv_artsizecode'])
    df_nfa= get_artikel_md()
    df=pd.merge(df,df_nfa,on='fv_barcode',how='left')
    df=df.loc[~df['fv_artsizecode'].isnull()]
    df2=df__artikel_pb("*")
    colist=df.columns.to_list()
    df[['fv_rating','fv_neo_link_foto']]=df[['fv_rating','fv_neo_link_foto']].fillna('none')
    df2[['fv_rating','fv_neo_link_foto']]=df2[['fv_rating','fv_neo_link_foto']].fillna('none')
    df['fv_ornal'] = df.apply(add_ornal, axis=1)
    df3=get_new_or_changed_rows(df, df2,subset=colist)
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
    'fv_ornal']]
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
    excel_path = f'{base_path}customers_pb_sup.xlsx'
    csv_path = f'{base_path}list user & employee.csv'
    pic_path = f'{base_path}LIST PIC MARKETING.xlsx'
    mic_path = f'{base_path}PIC MIC.xlsx'
    table_name = 'customers_pb'
    df = df__customers_pb_read()
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

    df_picmic=pd.read_excel(mic_path,).dropna(how='all')
    df=pd.merge(df,df_picmic[['PIC MIC','STORE GIS']],left_on='code',right_on='STORE GIS',how='left').fillna('-')
    df['Alokator']=df['PIC MIC']
    df.loc[df['fn_cusid']==915,'fn_whconsiid']= 9000
    df=df.drop(columns=['KODE TOKO GIS', 'MANAGER_x', 'SUPERVISOR/ OJT**','STORE GIS','PIC MIC'])
    df.loc[df['fn_grpcusid']==9,'Neo Toko']= "Bazar"
    df.loc[df['Toko'].str.startswith('RJ-FO'),'fc_codecus']=df['code']
    df.loc[df['fc_codecus'].str.startswith('-'),'fc_codecus']=df['code']
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