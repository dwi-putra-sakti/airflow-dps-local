
# from services.Queries_API
import time
import pandas as pd
import numpy as np
import sys
import json
import pytz
from rest_framework.response import Response
from rest_framework import status
from rest_framework.exceptions import ValidationError
from core.db import DatabaseConnector
from datetime import datetime, timedelta
from services.get_utils_API import (safe_print,timed, save_query_debug,list_to_sql_in,ProcessTimer,get_akses_data_toko)
import re
import os
import requests
from dotenv import load_dotenv
load_dotenv(dotenv_path="\\172.16.11.11\pythonApi\The Gabs\.env")
API_KEY = 'XF6X3x1Cc8t4LTPTWfJo'
dbLoad = 'default'
dbsave = 'local'
dbread = 'read'
def send_whatsapp(message):
    url = "https://api.fonnte.com/send"
    payload = {
        "target": "6281213335515",
        "message": message
    }
    
    headers = {
        "Authorization": API_KEY
    }

    print("Kirim WA dimulai...")
    print("API_KEY:", API_KEY)

    try:
        response = requests.post(url, data=payload, headers=headers, timeout=10)
        print("STATUS:", response.status_code)
        print("RESPONSE:", response.text)
        print(sys.executable)
        if response.status_code != 200:
            raise Exception("WA gagal")

        print("Kirim WA berhasil")

    except Exception as e:
        print("ERROR WA:", e)
        raise

def cek_update_db():
    year = datetime.now().year 
    year=str(year)[-2:]
    query = f'''
        SELECT
            table_name,
            update_time
        FROM information_schema.tables
        WHERE table_schema = 'gis_local' and update_time is not null and table_name like "%%{year}%%"
        ORDER BY update_time DESC
        '''
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    tz_jkt = pytz.timezone('Asia/Jakarta')
    today = datetime.now(tz_jkt).date()
    yesterday = (datetime.now() - timedelta(days=1)).date()

    col='update_time'
    name_col='table_name'
    df[col] = pd.to_datetime(df[col], errors='coerce')

    # Bandingkan apakah tanggal di kolom tidak sama dengan kemarin
    # invalid = df[df[col].dt.date != yesterday]
    invalid = df[df[col].dt.date != today]
    if not invalid.empty:
        print(f"ERROR: Data belum update untuk tanggal hari ini ({today})")
        print("Tabel bermasalah:", invalid[name_col].tolist())
        print("Harap cek Airflow")
        msg = f"Tabel bermasalah: {invalid[name_col].tolist()}"
        for _ in range(3):
            try:
                # send_whatsapp(msg)
                break
            except:
                time.sleep(5)
        print(yesterday)
        print(today)
        sys.exit(1)
        # raise ValidationError({"error": msg})
    else:
        return  

def sanitize_sql_text(text: str):
    """
    Jika aman → return text apa adanya.
    Jika berbahaya → return dict error.
    """

    if not text:
        return text  # tetap return original

    patterns = [
        r";",              # end-of-query
        r"--",             # comment injection
        r"/\*",            # block comment
        r"\bor\b",         # OR injection
        r"\band\b",        # AND injection
        r"drop\s+table",
        r"union\s+select",
        r"insert\s+into",
        r"delete\s+from",
        r"update\s+\w+"
    ]

    lower = text.lower()

    for p in patterns:
        if re.search(p, lower):
            return {
                "error": f"Peringatan: Query mengandung pola berbahaya: '{p}'",
                "input": text} 
    return text

def sanitize_many(**kwargs):
    """
    Memeriksa banyak variabel sekaligus.
    Jika ada yang berbahaya → return dict error.
    Jika aman semua → return dict nilai asli.
    """
    '''
    CONTOH PNEGGUNAAN 
    safe = sanitize_many(
        var_a=var_a,
        var_b=var_b
    )

    if isinstance(safe, dict) and "error" in safe:
        return safe  # langsung balikan error

    # variabel aman
    var_a = safe["var_a"]
    var_b = safe["var_b"]
    '''
    cleaned = {}

    for key, value in kwargs.items():
        result = sanitize_sql_text(value)
        if isinstance(result, dict):  # terdeteksi berbahaya
            # Tambahkan info variabel mana yang berbahaya
            result["field"] = key
            return result
        cleaned[key] = result

    return cleaned

# @timed
def generate_main_query(bulan_input):
    bulan_formatted = [f"{int(b.strip()):02d}" for b in bulan_input.split(',') if b.strip().isdigit()]
    stock_awal = f"SUM(fn_stock_{bulan_formatted[0]}) AS stock_awal"
    sales = f"SUM(" + " + ".join([f"fn_qty_{b}" for b in bulan_formatted]) + ") AS sales"
    bulan_akhir = f"{int(bulan_formatted[-1]) + 1:02d}"
    stock_akhir = f"SUM(fn_stock_{bulan_akhir}) AS stock_akhir"
    return f"{stock_awal}, {sales}, {stock_akhir}"

# @timed
def df__all_customers(col):
    query = f'''
            SELECT {col} FROM gis_local.customers_pb
            '''
        # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__customers_pb_(colom,distinct=False,where='',queri=False):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_local.customers_pb
        where 1=1 {where}
        '''
    if queri==True:print(query)
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__customers_pb_n(placeholders):
        query = f'''
            SELECT fn_cusid,fn_whconsiid FROM gis_local.customers_pb
            where fc_code in ({placeholders})
            '''
        # df = pd.read_sql(query, engine_imp)
        results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
        df = pd.DataFrame(results)
        return df

# @timed
def df__user_tb_(colom,distinct=False,where=''):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_db.user_tb
        where 1=1 {where}
        '''
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    df = pd.DataFrame(results)
    return df
# @timed
def df__userlvl_tb_(colom='a.fn_userid, fv_username,fv_nameemployees, fv_level, fv_namedepart, fn_cusid',distinct=False,where=''):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_db.user_tb a 
        left join gis_db.employees_tb b on a.fn_userid=b.fn_userid
        left join gis_db.department_tb c on b.fn_iddepart=c.fn_iddepart
        left join gis_db.t_userlevel d on a.fn_userlevel=d.fn_userlevel
left join gis_db.t_aksesusercust e on a.fn_userid=e.fn_userid
        where 1=1 {where}
        '''
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    df = pd.DataFrame(results)
    return df

# @timed
def df__drfatdtl_tb_(colom,distinct=False,where='',group_by='',queri=False):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_local.t_draftdtl
        where 1=1 {where}
        {group_by}
        '''
    
    if queri==True:print(query)
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__drfatmst_tb_(colom,distinct=False,where='',queri=False):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_local.t_draftmst
        where 1=1 {where}
        '''
    
    if queri==True:print(query)
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df
# @timed
def df__pre_order_dtl_(colom,distinct=False,where='',group_by='',queri=False):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_local.t_pre_order_dtl
        where 1=1 {where}
        {group_by}
        '''
    
    if queri==True:print(query)
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__pre_order_mst_(colom,distinct=False,where='',queri=False):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_local.t_pre_order_mst
        where 1=1 {where}
        '''
    
    if queri==True:print(query)
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df
# @timed
def df__user(colom,distinct=False,where='',queri=False):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_db.user_tb 
        where 1=1 {where}
        '''
    
    if queri==True:print(query)
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    df = pd.DataFrame(results)
    return df

# @timed
def df__toko_flag(colom,distinct=False,where='',queri=False):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_db.fl_stockarticleconsipostingmst 
        where 1=1 {where}
        '''
    
    if queri==True:print(query)
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    df = pd.DataFrame(results)
    return df

# @timed
def df__mstcompt(colom,distinct=False,where='',queri=False):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_local.data_kopetitor_2026 
        where 1=1 {where}
        '''
    
    if queri==True:print(query)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__mstcompt(colom,distinct=False,where='',queri=False):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_local.data_kopetitor_2026 
        where 1=1 {where}
        '''
    
    if queri==True:print(query)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__mstcompt_all(where='',queri=False):

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
    Select fc_code, fn_whconsiid, fv_brand, sum(fn_value) fn_value, fv_divname, fv_brandfor, fn_month from 
        (
        SELECT  
            fc_code, fn_whconsiid, fv_brand , 
            fn_value, fv_divname, fv_brandfor, fn_month
            FROM gis_local.data_kopetitor_2026 
            where 1=1 {where}
        union all
        SELECT  
            fc_code, fn_whconsiid,fv_brand_comp fv_brand , 
            fn_value, fv_divname, fv_brandfor, fn_month
            FROM gis_local.data_daily_competitor 
            where 1=1 {where}
        ) a
        group by fn_whconsiid, fv_brand, fv_divname, fv_brandfor, fn_month
        '''
    
    if queri==True:print(query)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__mstcompt_sales(where='',queri=False):
    # Mengambil tanggal hari ini
    hari_ini = datetime.now()
    # Format menjadi Tahun-Bulan
    hasil = hari_ini.strftime('%Y-%m')
    bulan = int(hari_ini.strftime('%m'))
    if where=='': where=''
    else: where=f'{where}'

    query = f'''
    Select fc_code, fn_whconsiid, fv_brand, fn_value, fv_divname, fv_brandfor, fn_month from 
        (SELECT fc_codecus fc_code, a.fn_whconsiid,fv_brandname fv_brandfor,  round((sum(fn_hargabruto)/1000000),2) as fn_value, fv_divname, 
        concat(fv_brandname," ",fv_divname) fv_brand, {bulan} as fn_month
        FROM gis_db.t_penjualandtl_goods a
        left join gis_db.article_tb b on a.fv_articlecode=b.fv_articlecode
        left join gis_db.brand_tb c on b.fn_brandid=c.fn_brandid
        left join gis_db.division_tb d on b.fn_divid=d.fn_divid
        left join gis_db.warehouseconsi_tb e on a.fn_whconsiid=e.fn_whconsiid
        left join gis_db.customer_tb f on e.fn_cusid=f.fn_cusid
        where 
        b.fn_brandid in (2,10) and b.fn_divid in (1,2) and fd_tgljual like "{hasil}%%" 
        and fn_grpcusid in (2,3) {where}
        group by fn_whconsiid, fv_brandname, fv_divname
        ) a
        '''
    
    if queri==True:print(query)
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    df = pd.DataFrame(results)
    return df

# @timed
def df__daily_compt(colom,distinct=False,where='',queri=False):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_local.data_daily_competitor 
        where 1=1 {where}
        '''
    
    if queri==True:print(query)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df  

# @timed
def df__group_alokator_(colom,distinct=False,where='',queri=False):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_local.group_alokator
        where 1=1 {where}
        '''
    
    if queri==True:print(query)
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__ket_table_(colom,distinct=False,where='',queri=False):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_local.master_store_art
        where 1=1 {where}
        '''
    
    if queri==True:print(query)
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__tmp_screensever(colom,distinct=False,where='',queri=False):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_local.tmp_set_monitoring_stock 
        where 1=1 {where}
        '''
    
    if queri==True:print(query)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df
    
# @timed
def add_where(input_param,cus='',art=''):
    q_alok=df__alok_from_subid(input_param,whconsi=True)
    fn_whcon=get_akses_data_toko(input_param,output='fn_whconsiid',cek_user_info=False)
    # print(q_alok)
    # print(json.dumps(input_param, indent=2, ensure_ascii=False))
    store_list = input_param['store_list']
    if store_list:  # hanya lanjut kalau ada isi (bukan None, '', atau [])
        # print('test1')
        placeholders = ", ".join([f"'{s.split(' ', 1)[0]}'" for s in store_list])
        df_cus = df__customers_pb_n(placeholders)
        whconsi_list = df_cus['fn_whconsiid'].tolist()

        if whconsi_list:
            whconsi_placeholders = ", ".join([str(s) for s in whconsi_list])
            whconsi = f"and {cus}fn_whconsiid IN ({whconsi_placeholders})"
        else:
            whconsi = ""
    elif q_alok:
        # print('test2')
        whconsi= f"and {cus}fn_whconsiid IN ({q_alok})"
    elif fn_whcon:
        # print('test3')
        whconsi= f"and {cus}fn_whconsiid IN ({fn_whcon})"
    else:
        whconsi = ""

    # print(whconsi)
    if input_param['divisi'] is not None and input_param['divisi']!='':
        divisi = input_param['divisi']
        divisi = f'and {art}fv_divname="{divisi}"'
    else: divisi = ''

    if input_param['ornal'] is not None and input_param['ornal']!='':
        ornal = input_param['ornal']
        ornal = f'and {art}fv_ornal="{ornal}"'
    else: ornal = ''

    if input_param['cat'] is not None and input_param['cat']!='':
        cat = input_param['cat']
        cat = f'and {art}fv_catname="{cat}"'
    else: cat = ''

    if input_param['artikel'] is not None and input_param['artikel']!='':
        artikel = input_param['artikel']
        artikel = f'and {art}fv_barcode="{artikel}"'
    else: artikel = ''

    if input_param['spv_list']: 
        spv_list = input_param['spv_list']
        if spv_list:
            spv_placeholders = ", ".join([f"'{s}'" for s in spv_list])
            spv=f'and {cus}fv_spv IN ({spv_placeholders})'
        else: spv= ''
    else: spv = ''

    if input_param['brand'] is not None and input_param['brand']!='':
            brand = input_param['brand']
            brand = f'and {art}fv_brand="{brand}"'
    else: brand= ''

    if input_param['artikelsize']: 
        artikelsize = input_param['artikelsize']
        if artikelsize:
            artikelsize=f'and {art}fv_artsizecode IN ("{artikelsize}")'
        else: artikelsize= ''
    else: artikelsize = ''

    if input_param['list_artsize']: 
        list_artsize = input_param['list_artsize']
        if list_artsize:
            list_art_placeholders = ", ".join([f"'{s}'" for s in list_artsize])
            list_artsize=f'and {art}fv_artsizecode IN ({list_art_placeholders})'
        else: list_artsize= ''
    else: list_artsize = ''

    if 'sub_cat' in input_param and input_param['sub_cat'] is not None and str(input_param['sub_cat']).strip():
        sub_cat = input_param['sub_cat']
        cek_sub=sub_cat
        sub_cat = f"and ({art}fv_fitting='{sub_cat}' or {art}motif='{sub_cat}' )"
        
        if cek_sub in ['BLACK', 'RED', 'WHITE']:
            sub_cat = f'and {art}fv_colorname = "{cek_sub}" and {art}motif="POLOS"'
        elif cek_sub == "LAIN-LAIN":
            sub_cat = f'and {art}fv_colorname not in ("BLACK","RED","WHITE") and {art}motif="POLOS"'
    else: sub_cat = ''

    return {
        'whconsi':whconsi,
        'divisi':divisi,
        'ornal':ornal,
        'cat':cat,
        'artikel':artikel,
        'spv':spv,
        'sub_cat':sub_cat,
        'brand':brand,
        'artikelsize':artikelsize,
        'list_artsize':list_artsize
    }

# @timed
def df__customers_pb(placeholders,colom):
    query = f'''
        SELECT {colom} FROM gis_local.customers_pb
        where fc_code in ({placeholders})
        '''
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__cus_list_get_all_distinct(colom):
    query = f'''
        SELECT distinct {colom} FROM gis_local.customers_pb
        '''
        # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__whconsiid_from_alokator_spv(list_selected,list_select):
    if list_selected=='alokator':
        query = f'''
            SELECT DISTINCT concat(fc_code,' - ', fv_toko)  fv_toko, fn_whconsiid, fc_code FROM gis_local.customers_pb
            where fv_alokator in ({list_select})
            '''
    else: query = f'''
            SELECT DISTINCT concat(fc_code,' - ', fv_toko)  fv_toko, fn_whconsiid, fc_code FROM gis_local.customers_pb
            where fv_spv in ({list_select})
            '''
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__select_articlesize_from_list(input_param,list_art=None):
    if input_param['artikel']==None or input_param['artikel']=="": 
        if list_art is None:
            list_art=''
        else:
            list_art=list_art
            list_art = ",".join([f"'{x}'" for x in list_art])
            list_art =f'and fv_barcode in ({list_art})'
    else: 
        list_art=f"'{input_param['artikel']}'"
        list_art =f'and fv_barcode in ({list_art})'
    query = f'''
        SELECT fv_artsizecode FROM gis_local.artikel_pb 
        WHERE 1=1 {list_art}
    '''
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    
    if df.empty:
        return []
    
    return df['fv_artsizecode'].tolist()

# @timed
def df__select_articlesize_from_artikel(input_param):
    artikel = input_param['artikel']
    if not artikel:
        return []

    query = f'''
        SELECT fv_artsizecode FROM gis_local.artikel_pb 
        WHERE fv_barcode = "{artikel}"
    '''
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)

    if df.empty:
        return []
    
    return df['fv_artsizecode'].tolist()

# @timed
def df_artikel_pb_(colom,distinct=False,where='',queri=False):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_local.artikel_pb
        where 1=1 {where}
        '''
    
    if queri==True:print(query)
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__retur_req(input_param, df,db):
    store_list = input_param['store_list']
    if store_list:
        placeholders = ", ".join([f"'{s}'" for s in store_list])
    df_cus = df__customers_pb_n(placeholders)
    cus_list= df_cus['fn_cusid'].tolist()
    cus_list =  ", ".join([f"'{s}'" for s in cus_list])
    whconsi_list=df_cus['fn_whconsiid'].tolist()
    whconsi_list =  ", ".join([f"'{s}'" for s in whconsi_list])
    query = f'''
        SELECT fn_whconsiid, fn_qtykirim,fn_qtyverified , fv_artsizecode, t.fc_status from t_returdtl_pre t
        LEFT JOIN t_returmst_pre m on t.fv_noretur = m.fv_noretur
        WHERE t.fc_status in ('T','X') and fn_whconsiid in ({whconsi_list})
        '''
    results = DatabaseConnector.execute_raw_sql(query, using=db)
    freestok_df = pd.DataFrame(results)
    if freestok_df.empty:
        return df
    else:
        freestok_df['stok_f']=np.where(freestok_df['fc_status'] == "X", freestok_df['fn_qtyverified'], freestok_df['fn_qtykirim'])  
        freestok_df=freestok_df[['fn_whconsiid','stok_f','fv_artsizecode']]
        df=pd.merge(df, freestok_df, on=['fn_whconsiid', 'fv_artsizecode'], how='left').fillna(0)
        df['stocks']=df['stocks'].astype(int)
        df['stok_f']=df['stok_f'].astype(int)
        df['stocks']=df['stocks']-df['stok_f']
        df=df.drop(columns=['stok_f'])
        return df
    
# @timed
def df__main(input_param,df=None,list_from='',queri=False):
    input_value = input_param['input_date']
    store_list = input_param['store_list']
    if store_list:
        placeholders = ", ".join([f"'{s}'" for s in store_list])
    df_cus = df__customers_pb_n(placeholders)
    cus_list= df_cus['fn_cusid'].tolist()
    cus_list =  ", ".join([f"'{s}'" for s in cus_list])
    whconsi_list=df_cus['fn_whconsiid'].tolist()
    whconsi_list =  ", ".join([f"'{s}'" for s in whconsi_list])
    sales_thru = f"sales_thru_pb_{input_value[2:4]}"
    trxstx = f"trxstx_pb{input_value[2:4]}"
    stock = f"fn_stock_{input_value[5:7]}"
    where=add_where(input_param,cus='', art='b.')
    divisi=where['divisi']
    ornal=where['ornal']
    if df is not None and list_from:
        artikel = df[list_from].dropna().unique().tolist()
        if artikel:  # cek kalau list tidak kosong
            list_artsize = ",".join([f"'{x}'" for x in artikel])
            list_artsize = f"AND {list_from} IN ({list_artsize})"
        else:
            list_artsize = ""
    else:
        list_artsize = ""

    main_query = f"""
    SELECT 
    stock_backdate.fn_whconsiid,
    SUM(stock) AS stocks,
    stock_backdate.fv_artsizecode, 
    fv_barcode, fv_sizename, fv_configname

    FROM (
    SELECT fn_whconsiid, {stock} AS stock, fv_artsizecode 
    FROM gis_local.{sales_thru} where fn_whconsiid in ({whconsi_list})
    UNION ALL
    SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
    FROM gis_local.{trxstx} 
    WHERE fd_tgltrx BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND '{input_value}'
    and fn_whconsiid in ({whconsi_list}) {list_artsize}


    UNION ALL
    SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
    FROM gis_local.trxrt_pb
    WHERE fd_tgltrx <= '{input_value}' and fv_stat in ( 'adj_out', 'adj_in','recieve')
    and fn_whconsiid in ({whconsi_list}) {list_artsize}

    UNION ALL
    SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
    FROM gis_local.trxrt_pb
    WHERE fd_tgltrx BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND '{input_value}' and fv_stat not in  ( 'adj_out', 'adj_in','recieve')
    and fn_whconsiid in ({whconsi_list}) {list_artsize}

    UNION ALL
    SELECT fn_whconsiid, fv_freestock AS stock, fv_artsizecode 
    FROM gis_local.Freestock_varian_{input_value[2:4]}
    WHERE fd_tgl BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND '{input_value}'
    and fn_whconsiid in ({whconsi_list}) {list_artsize}
    ) stock_backdate
    left join artikel_pb b on stock_backdate.fv_artsizecode = b.fv_artsizecode
    WHERE 1=1 {divisi} {ornal} 
    GROUP BY fn_whconsiid, fv_artsizecode
    """
    
    if queri==True: print(main_query)
    # df = pd.read_sql(main_query, engine_imp)
    # Retry up to 5 times if error
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
            df = pd.DataFrame(results)
            return df  # success, langsung return
        except Exception as e:
            print(f"[Attempt {attempt}] Query gagal: {e}")
            if attempt == max_retries:
                raise  # kalau sudah max retry, lempar error
            time.sleep(2)  # tunggu 2 detik sebelum nyoba lagi
    # results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
    # df = pd.DataFrame(results)
    # return df

# @timed
def df__main_stok_atas(input_param,cat=None):
    input_value = input_param['input_date']
    store_list = input_param['store_list']
    if store_list:
        placeholders = ", ".join([f"'{s}'" for s in store_list])
    df_cus = df__customers_pb_n(placeholders)
    cus_list= df_cus['fn_cusid'].tolist()
    cus_list =  ", ".join([f"'{s}'" for s in cus_list])
    whconsi_list=df_cus['fn_whconsiid'].tolist()
    whconsi_list =  ", ".join([f"'{s}'" for s in whconsi_list])
    cat=input_param['cat']
    if cat is not None and cat!='':
        cat = ", ".join([f"'{x}'" for x in cat])
        cat=f'and fv_catname in ({cat})'
    else:
        cat = ''
    sales_thru = f"sales_thru_pb_{input_value[2:4]}"
    trxstx = f"trxstx_pb{input_value[2:4]}"
    stock = f"fn_stock_{input_value[5:7]}"
    main_query = f"""
        SELECT 
            stock_backdate.fn_whconsiid,
            SUM(stock) AS stocks,
            stock_backdate.fv_artsizecode,
            a.fv_barcode,
            a.fv_catname,
            a.fv_brandname,
            a.fv_divname,
            a.fv_sizename,
            a.fv_configname,
            a.fv_namemark,
            a.fm_price,
            b.fc_code,
            concat(fc_code, ' - ', fv_toko) fv_toko,
            a.fv_rating,
            fv_colorname
        FROM (
            SELECT fn_whconsiid, {stock} AS stock, fv_artsizecode 
            FROM gis_local.{sales_thru}
            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
            FROM gis_local.{trxstx} 
            WHERE fd_tgltrx BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND '{input_value}'
            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
            FROM gis_local.trxrt_pb
            WHERE fd_tgltrx < '{input_value}'
            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
            FROM gis_local.Stock_varian_25
            WHERE fd_tgl BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND '{input_value}'
        ) stock_backdate
        LEFT JOIN artikel_pb a ON a.fv_artsizecode = stock_backdate.fv_artsizecode
        LEFT JOIN customers_pb b ON stock_backdate.fn_whconsiid = b.fn_whconsiid
        WHERE 1=1 and stock_backdate.fn_whconsiid in ({whconsi_list}) {cat}
        GROUP BY stock_backdate.fn_whconsiid, fv_artsizecode
        """
    # Retry up to 5 times if error
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
            df = pd.DataFrame(results)
            return df  # success, langsung return
        except Exception as e:
            print(f"[Attempt {attempt}] Query gagal: {e}")
            if attempt == max_retries:
                raise  # kalau sudah max retry, lempar error
            time.sleep(2)  # tunggu 2 detik sebelum nyoba lagi
    # results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
    # df = pd.DataFrame(results)
    # return df

# @timed
def df__main_filt_arts(input_param,list_art=None,list_from='',queri=False):
    q_alok=df__alok_from_subid(input_param)
    input_value = input_param['input_date']
    # Konversi kembali ke string
    sales_thru = f"sales_thru_pb_{input_value[2:4]}"
    trxstx = f"trxstx_pb{input_value[2:4]}"
    stock = f"fn_stock_{input_value[5:7]}"
    # # printol(input_param)
    artikel=input_param['artikel']
    if list_art is not None and list_from:
        artikel = list_art
        if artikel:  # cek kalau list tidak kosong
            list_artsize = ",".join([f"'{x}'" for x in artikel])
            list_artsize = f"AND {list_from} IN ({list_artsize})"
        else:
            list_artsize = ""
    else:
        list_artsize = f"AND fv_barcode in ('{artikel}')"
    where=add_where(input_param)
    spv_list=where['spv']
    main_query = f"""
    SELECT 
        stock_backdate.fn_whconsiid,
        SUM(stock) AS stocks,
        stock_backdate.fv_artsizecode,
        a.fv_barcode,
        a.fv_sizename,
        a.fv_configname,
        a.fv_namemark,
        b.fc_code,
        concat(fc_code, ' - ', fv_toko) fv_toko
    FROM (
        SELECT c.fn_whconsiid, {stock} AS stock, c.fv_artsizecode 
        FROM gis_local.{sales_thru} c
        LEFT JOIN artikel_pb a ON c.fv_artsizecode = a.fv_artsizecode
        LEFT JOIN customers_pb b ON c.fn_whconsiid = b.fn_whconsiid
        WHERE 1=1 {list_artsize} {q_alok} {spv_list}

        UNION ALL
        SELECT c.fn_whconsiid, fn_stock AS stock, c.fv_artsizecode 
        FROM gis_local.{trxstx} c
        LEFT JOIN artikel_pb a ON c.fv_artsizecode = a.fv_artsizecode
        LEFT JOIN customers_pb b ON c.fn_whconsiid = b.fn_whconsiid
        WHERE fd_tgltrx BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND '{input_value}'
        {list_artsize} {q_alok} {spv_list}
        
        UNION ALL
        SELECT c.fn_whconsiid, fn_stock AS stock, c.fv_artsizecode 
        FROM gis_local.trxrt_pb c
        LEFT JOIN artikel_pb a ON c.fv_artsizecode = a.fv_artsizecode
        LEFT JOIN customers_pb b ON c.fn_whconsiid = b.fn_whconsiid
        WHERE fd_tgltrx <= '{input_value}' and (fv_stat = 'adj_out' or fv_stat = 'adj_in')
        {list_artsize} {q_alok} {spv_list}

        UNION ALL
        SELECT c.fn_whconsiid, fn_stock AS stock, c.fv_artsizecode 
        FROM gis_local.trxrt_pb c
        LEFT JOIN artikel_pb a ON c.fv_artsizecode = a.fv_artsizecode
        LEFT JOIN customers_pb b ON c.fn_whconsiid = b.fn_whconsiid
        WHERE fd_tgltrx BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND '{input_value}'
        {list_artsize} {q_alok} {spv_list}
        
        UNION ALL
        SELECT c.fn_whconsiid, fn_stock AS stock, c.fv_artsizecode 
        FROM gis_local.Stock_varian_25 c
        LEFT JOIN artikel_pb a ON c.fv_artsizecode = a.fv_artsizecode
        LEFT JOIN customers_pb b ON c.fn_whconsiid = b.fn_whconsiid
        WHERE fd_tgl BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND '{input_value}'
        {list_artsize} {q_alok} {spv_list}
        
        UNION ALL
        SELECT c.fn_whconsiid, fn_stock AS stock, c.fv_artsizecode 
        FROM gis_local.Stock_varian_26 c
        LEFT JOIN artikel_pb a ON c.fv_artsizecode = a.fv_artsizecode
        LEFT JOIN customers_pb b ON c.fn_whconsiid = b.fn_whconsiid
        WHERE fd_tgl BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND '{input_value}'
        {list_artsize} {q_alok} {spv_list}
    ) stock_backdate
    LEFT JOIN artikel_pb a ON stock_backdate.fv_artsizecode = a.fv_artsizecode
    LEFT JOIN customers_pb b ON stock_backdate.fn_whconsiid = b.fn_whconsiid
    GROUP BY fn_whconsiid, fv_artsizecode
    having stocks > 0
    """
    if queri==True: print(main_query)
    # df = pd.read_sql(main_query, engine_imp)
    # Retry up to 5 times if error
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
            df = pd.DataFrame(results)
            return df  # success, langsung return
        except Exception as e:
            print(f"[Attempt {attempt}] Query gagal: {e}")
            if attempt == max_retries:
                raise  # kalau sudah max retry, lempar error
            time.sleep(2)  # tunggu 2 detik sebelum nyoba lagi
    # results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
    # df = pd.DataFrame(results)
    # return df

# @timed
def df__main_freestock(input_param):
    input_value = input_param['input_date']
    input_now = datetime.today().strftime("%Y-%m-%d")
    # Konversi kembali ke string
    sales_thru = f"sales_thru_pb_{input_value[2:4]}"
    trxstx = f"trxstx_pb{input_value[2:4]}"
    stock = f"fn_stock_{input_value[5:7]}"
    artikel=input_param['artikel']
    main_query = f"""
    SELECT 
        stock_backdate.fn_whconsiid,
        SUM(stock) AS freestocks,
        stock_backdate.fv_artsizecode
    FROM (
        SELECT fn_whconsiid, {stock} AS stock, fv_artsizecode 
        FROM gis_local.{sales_thru}
        UNION ALL
        SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
        FROM gis_local.{trxstx} 
        WHERE fd_tgltrx BETWEEN '{input_value}' AND '{input_now}'
        UNION ALL
        SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
        FROM gis_local.trxrt_pb <= '{input_value}'
        UNION ALL
        SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
        FROM gis_local.Stock_varian_25
        WHERE fd_tgl BETWEEN '{input_value}' AND '{input_now}'
    ) stock_backdate
    LEFT JOIN artikel_pb a ON a.fv_artsizecode = stock_backdate.fv_artsizecode
    GROUP BY fn_whconsiid, fv_artsizecode
    """
    # df = pd.read_sql(main_query, engine_imp)
    # Retry up to 5 times if error
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
            df = pd.DataFrame(results)
            return df  # success, langsung return
        except Exception as e:
            print(f"[Attempt {attempt}] Query gagal: {e}")
            if attempt == max_retries:
                raise  # kalau sudah max retry, lempar error
            time.sleep(2)  # tunggu 2 detik sebelum nyoba lagi
    # results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
    # df = pd.DataFrame(results)
    # return df

# @timed
def df__main_draft(input_param):
    store_list = input_param['store_list']
    if store_list:
        placeholders = ", ".join([f"'{s}'" for s in store_list])
    df_cus = df__customers_pb_n(placeholders)
    cus_list= df_cus['fn_cusid'].tolist()
    cus_list =  ", ".join([f"'{s}'" for s in cus_list])
    whconsi_list=df_cus['fn_whconsiid'].tolist()
    whconsi_list =  ", ".join([f"'{s}'" for s in whconsi_list])

    main_query = f"""
    SELECT 
    fn_whcosiid_asal fn_whconsiid,
    SUM(fn_qtykirim) AS stocks,
    a.fv_artsizecode, fv_barcode, fv_sizename
    from gis_local.t_draftdtl a
    left join artikel_pb b on a.fv_artsizecode = b.fv_artsizecode
    WHERE 1=1 and fc_status= "T" and fc_type = "mt_in" and fn_whcosiid_asal in ({whconsi_list})
    GROUP BY fn_whcosiid_asal , a.fv_artsizecode
    """
    results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__main_bedah_counter(input_param):
    if year is None: year = datetime.now().year 
    else: year = int(year)
    store_list =input_param['store_list']
    if store_list  is not None:
        if store_list:
            placeholders = ", ".join([f"'{s}'" for s in store_list])
            df_cus = df__customers_pb_n(placeholders)
            whconsi_list = df_cus['fn_whconsiid'].tolist()
            
            if whconsi_list:
                whconsi_placeholders = ", ".join([str(s) for s in whconsi_list])
                whconsi = f"AND fn_whconsiid IN ({whconsi_placeholders})"
            else: whconsi = ""  # atau kondisi default kalau kosong
    else: whconsi = ""  # jika input_param['store_list'] None
    if input_param['divisi'] is not None:
        divisi = input_param['divisi']
        divisi = f'and fv_divname="{divisi}"'
    else: divisi = ''
    bulan=input_param['bulan']
    main_=generate_main_query(bulan)
    sales_thru=f'sales_thru_pb_{str(year)[-2:]}'
    main_query = f"""
    select
    sales_thru.fv_catname kategory,
    sales_thru.stock_awal,
    sales_thru.total_stock,
    sales_thru.sales,
    ROUND(100.0 * sales_thru.sales / NULLIF(total_data.sales, 0), 2) AS sales_kontribusi,
        sales_thru.stock_akhir,
    ROUND(100.0 * sales_thru.stock_akhir / NULLIF(total_data.stock_akhir, 0), 2) AS stock_kontribusi
    from (

        select fv_catname , 
        CAST(stock_awal AS SIGNED) AS stock_awal, 
        CAST(stock_akhir + sales AS SIGNED) AS total_stock,
        CAST(sales AS SIGNED) AS sales, 
        CAST(stock_akhir AS SIGNED) AS stock_akhir from (

        select fv_catname, {main_}
            FROM gis_local.{sales_thru} a
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
            WHERE 1=1 {whconsi} {divisi}
            group by fv_catname
        union all

        select "TOTAL" as fv_catname, {main_}
            FROM gis_local.{sales_thru} a
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
            WHERE 1=1 {whconsi} {divisi} )sub
            ) sales_thru,
            (
                select fv_catname, {main_}
                FROM gis_local.{sales_thru} a
                left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
                WHERE 1=1 {whconsi} {divisi} 
            ) total_data
    """
    results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__aging(input_param):
    input_value = input_param['input_date']
    store_list = input_param['store_list']
    if store_list:
        placeholders = ", ".join([f"'{s}'" for s in store_list])
    df_cus = df__customers_pb_n(placeholders)
    whconsi_list=df_cus['fn_whconsiid'].tolist()
    whconsi_list =  ", ".join([f"'{s}'" for s in whconsi_list])

    query = f"""
    SELECT a.fn_whconsiid, fv_barcode, fn_stock, fd_tgltrx FROM 
    (SELECT * FROM gis_local.trxstx_pb24 
    where fv_stat like 'receive%%' and fd_tgltrx <= '{input_value}'
    union all
    SELECT fn_whconsiid, fv_artsizecode, fn_stock, fd_tgltrx, fv_stat FROM gis_local.trxstx_pb25 
    where fv_stat like 'receive%%' and fd_tgltrx <= '{input_value}') a
    left join artikel_pb b on a.fv_artsizecode = b.fv_artsizecode
    WHERE 1=1 and fn_whconsiid in ({whconsi_list})
    group by fn_whconsiid,fv_barcode, fd_tgltrx 
    """
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__aging_filt_art(input_param):
    input_value = input_param['input_date']
    artikel=input_param['artikel']
    list_artsize=''
    if input_param['artikel']:
        artikel={input_param['artikel']}
        artikel = ", ".join(f"'{x}'" for x in artikel)
        df_art=df_artikel_pb_('fv_artsizecode',distinct=True,where=f'and fv_barcode in ({artikel})')
        list_artsize=list_to_sql_in(df_art,'fv_artsizecode')
        list_artsize=f'and fv_artsizecode in ({list_artsize})'
    query = f"""
    SELECT a.fn_whconsiid, fv_barcode, fn_stock, fd_tgltrx FROM 
    (SELECT fn_whconsiid, fv_artsizecode, fn_stock, fd_tgltrx, fv_stat FROM gis_local.trxstx_pb24 a
    WHERE 1=1 {list_artsize}
    and fv_stat like 'receive%%' and fd_tgltrx <= '{input_value}'
    union all

    SELECT fn_whconsiid, fv_artsizecode, fn_stock, fd_tgltrx, fv_stat FROM gis_local.trxstx_pb25 
    WHERE 1=1 {list_artsize} and fv_stat like 'receive%%' and fd_tgltrx <= '{input_value}'
    ) a
    left join artikel_pb b on a.fv_artsizecode = b.fv_artsizecode
    WHERE 1=1 and fv_barcode in ({artikel})
    group by fn_whconsiid,fv_barcode, fd_tgltrx 
    """
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

def df__aging_fdo(colom,distinct=False,where='',queri=False):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'
    query = f"""
    SELECT  {distinct} {colom} FROM gis_local.aging_gudang_pb a
    where 1=1 {where}
    """
    if queri==True:print(query)
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

def df__aging_toko(colom,distinct=False,where='',queri=False):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'
    query = f"""
    SELECT  {distinct} {colom} FROM gis_local.aging_pb a
    where 1=1 {where}
    """
    if queri==True:print(query)
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__sales(input_param,gb='fv_barcode'):
    start_str = input_param['start']
    ends_str = input_param['end']
    alokator = input_param['alokator']
    store_list = input_param['store_list']
    if store_list:
        placeholders = ", ".join([f"'{s}'" for s in store_list])
    df_cus = df__customers_pb_n(placeholders)
    whconsi_list=df_cus['fn_whconsiid'].tolist()
    whconsi_list =  ", ".join([f"'{s}'" for s in whconsi_list])

    query = f"""
        select fv_barcode, sum(fn_jualpersize) as sales, fn_jualpersize, fv_sizename, fn_whconsiid, trx.fv_artsizecode from (
        SELECT fv_barcode, fn_jualpersize, fd_tgltrx, fv_sizename, fn_whconsiid, a.fv_artsizecode FROM gis_local.pb_penjualan_2026 a
        left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
        where fd_tgltrx between '{ends_str}' and '{start_str}' and fn_whconsiid in ({whconsi_list})
        union all 
        SELECT fv_barcode, fn_jualpersize, fd_tgltrx, fv_sizename, fn_whconsiid, a.fv_artsizecode FROM gis_local.pb_penjualan_2025 a
        left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
        where fd_tgltrx between '{ends_str}' and '{start_str}' and fn_whconsiid in ({whconsi_list})
        union all 
        SELECT fv_barcode, fn_jualpersize, fd_tgltrx, fv_sizename, fn_whconsiid, a.fv_artsizecode FROM gis_local.pb_penjualan_2024 a
        left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        where fd_tgltrx between '{ends_str}' and '{start_str}' and fn_whconsiid in ({whconsi_list})
        union all 
        SELECT fv_barcode, fn_jualpersize, fd_tgltrx, fv_sizename, fn_whconsiid, a.fv_artsizecode FROM gis_local.pb_penjualan_today a
        left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        where fd_tgltrx between '{ends_str}' and '{start_str}' and fn_whconsiid in ({whconsi_list})
        ) trx
         
        group by {gb}, fn_whconsiid      
        """
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    # df = pd.read_sql(query, engine_imp)
    return df

# @timed
def df__sales_filt_art(input_param):
    start_str = input_param['start']
    ends_str = input_param['end']
    artikel=input_param['artikel']
    query = f"""
        select fv_barcode, sum(fn_jualpersize) as sales, fn_jualpersize, fv_sizename, fn_whconsiid, trx.fv_artsizecode from (
        
        SELECT fv_barcode, fn_jualpersize, fd_tgltrx, fv_sizename, fn_whconsiid, a.fv_artsizecode FROM gis_local.pb_penjualan_2026 a
        left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode where fv_barcode in ('{artikel}')
        union all 
        SELECT fv_barcode, fn_jualpersize, fd_tgltrx, fv_sizename, fn_whconsiid, a.fv_artsizecode FROM gis_local.pb_penjualan_2025 a
        left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode where fv_barcode in ('{artikel}')
        union all 
        SELECT fv_barcode, fn_jualpersize, fd_tgltrx, fv_sizename, fn_whconsiid, a.fv_artsizecode FROM gis_local.pb_penjualan_2024 a
        left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode where fv_barcode in ('{artikel}')) trx
        where trx.fd_tgltrx between '{ends_str}' and '{start_str}' 
        group by fv_barcode, fn_whconsiid
        """
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    # df = pd.read_sql(query, engine_imp)
    return df

# @timed
def df__retur(input_param,
              year=None,
              col='a.fn_whconsiid, a.fv_barcode,',
              gb='a.fn_whconsiid, a.fv_barcode',
              selected_month=False,
              queri=False):
    if year is None: year = datetime.now().year 
    start_str = str(year) + (input_param['start'])[4:]
    ends_str = str(year) + (input_param['end'])[4:]
    # input_value = input_param['input_date']
    where=add_where(input_param,cus='a.', art='b.')
    whconsi_list=where['whconsi']
    divisi=where['divisi']
    cat=where['cat']
    artikel=where['artikel']
    ornal=where['ornal']
    sub_cat=where['sub_cat']
    fd_date=f"and fd_tgltrx between '{ends_str}' and '{start_str}' "
    if selected_month==True:
        input_month=input_param['month_sales_ly']
        month_list = [int(x) for x in input_month.split(',')]
        fd_date = f'''and({'or '.join([f'fd_tgltrx like "{year}-{m:02d}-%%" ' for m in month_list])})'''
    query = f"""
    SELECT {col} sum(fn_stock * -1) retur FROM 
        (
        SELECT a.fn_whconsiid, a.fv_artsizecode, fn_stock, fv_barcode from gis_local.trxstx_pb24 a
        left join  gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        left join gis_local.customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        where 1=1 {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat} and fv_stat="retur"
        {fd_date} 

        union all
        SELECT a.fn_whconsiid, a.fv_artsizecode, fn_stock, fv_barcode from gis_local.trxstx_pb25 a
        left join  gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        left join gis_local.customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        where 1=1 {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat} and fv_stat="retur"
        {fd_date} 

        union all  
        SELECT a.fn_whconsiid, a.fv_artsizecode, fn_stock, fv_barcode from gis_local.trxstx_pb26 a
        left join  gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        left join gis_local.customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        where 1=1 {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat} and fv_stat="retur"
        {fd_date} 
        ) a
        left join  gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        left join gis_local.customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        group by {gb}
    """
    if queri==True:print(query)
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    if not df.empty:
        df['retur'] = df['retur'].astype(int)
    else: df=pd.DataFrame(columns=['fn_whconsiid','fv_barcode','retur'])
    # df = pd.read_sql(query, engine_imp)
    return df

# @timed
def df__retur_filt_art(input_param):
    input_value = input_param['input_date']
    alokator = input_param['alokator']
    artikel=input_param['artikel']
    query = f"""
    SELECT a.fn_whconsiid, fv_barcode, sum(fn_stock * -1) retur FROM 
        (SELECT a.fn_whconsiid, a.fv_artsizecode, fn_stock, fv_barcode from gis_local.trxstx_pb24 a
        left join  gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        left join customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        where 1=1 and fv_barcode in ('{artikel}') and fv_stat="retur"
        and fd_tgltrx <= '{input_value}'

        union all  
        SELECT a.fn_whconsiid, a.fv_artsizecode, fn_stock, fv_barcode from gis_local.trxstx_pb25 a
        left join  gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        left join customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        where 1=1 and fv_barcode in ('{artikel}') and fv_stat="retur"
        and fd_tgltrx <= '{input_value}') a
        
        group by fn_whconsiid, fv_barcode
    """
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    
    if not df.empty:
        df['retur'] = df['retur'].astype(int)
    else: df=pd.DataFrame(columns=['fn_whconsiid','fv_barcode','retur'])
    # df = pd.read_sql(query, engine_imp)
    return df

# @timed
def df__retur_between(input_param):
    year=2025
    where=add_where(input_param,cus='', art='')
    if year is None: year = datetime.now().year 
    times='this_y'
    if times == 'this_y':
        start_str = str(year) + (input_param['start'])[4:]
        ends_str = str(year) + (input_param['end'])[4:]
    else: 
        start_str = str(year) + (input_param['start_last'])[4:]
        ends_str = str(year) + (input_param['end_last'])[4:]
    whconsi_list=where['whconsi']
    divisi=where['divisi']
    cat=where['cat']
    artikel=where['artikel']
    ornal=where['ornal']
    if any([divisi, cat, ornal, artikel]):  
        # ada salah satu yang terisi
        df_art = df_artikel_pb_(
                    'fv_barcode',
                    distinct=True,
                    where=f'{cat} {artikel} {ornal} {divisi}'
                )
        artikel = df_art['fv_barcode'].dropna().unique().tolist()
        list_art = ",".join([f"'{x}'" for x in artikel])
        list_art = f'and fv_barcode in ({list_art})'
    else:
        # semuanya kosong
        list_art = ''
    query=f"""SELECT 
    fn_whconsiid, fv_artsizecode, sum(fn_qtykirim)*-1 as retur  FROM gis_db.t_returdtl a 
    inner join gis_db.t_returmst b on a.fv_noretur=b.fv_noretur 
    left join gis_db.article_tb c on a.fn_articleid=c.fn_articleid
    where b.fc_status <> "F" and fd_tgltransaksi between '{ends_str}' and '{start_str}' {whconsi_list} {list_art}
    group by fn_whconsiid, fv_artsizecode
    """
    # query = f"""
    # select a.fn_whconsiid, fv_barcode, sum(qty)*-1 as retur FROM gis_local.trx_retur a
    # left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
    # left join customers_pb c on a.fn_whconsiid=c.fn_whconsiid
    # WHERE tgltrx between '{ends_str}' and '{start_str}' {whconsi_list} {list_art}
    # group by fv_toko, fv_barcode
    # """
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbsave))
    # df = pd.read_sql(query, engine_imp)
    return df

# @timed
def df__mutasi(input_param,
              col='a.fn_whconsiid, a.fv_barcode,',
              gb='a.fn_whconsiid, a.fv_barcode',
              queri=False):
    input_value = input_param['input_date']
    where=add_where(input_param,cus='a.', art='')
    whconsi_list=where['whconsi']
    divisi=where['divisi']
    cat=where['cat']
    artikel=where['artikel']
    ornal=where['ornal']
    sub_cat=where['sub_cat']

    query = f"""
        SELECT {col} sum(fn_stock * -1) Mutasi FROM 
        (
        SELECT a.fn_whconsiid, a.fv_artsizecode, fn_stock, fv_barcode from gis_local.trxstx_pb24 a
        left join gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        left join gis_local.customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        where 1=1 {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat} and fv_stat="mutasi"
        and fd_tgltrx <= '{input_value}'

        union all  
        SELECT a.fn_whconsiid, a.fv_artsizecode, fn_stock, fv_barcode from gis_local.trxstx_pb25 a
        left join gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        left join gis_local.customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        where 1=1 {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat} and fv_stat="mutasi"
        and fd_tgltrx <= '{input_value}'

        union all  
        SELECT a.fn_whconsiid, a.fv_artsizecode, fn_stock, fv_barcode from gis_local.trxstx_pb26 a
        left join gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        left join customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        where 1=1 {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat} and fv_stat="mutasi"
        and fd_tgltrx <= '{input_value}') a
        left join gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        left join customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        group by {gb}
        """
    
    if queri==True:print(query)
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    # df = pd.read_sql(query, engine_imp)
    if not df.empty:
        df['Mutasi']=df['Mutasi'].astype(int)
    else: df=pd.DataFrame(columns=['fn_whconsiid','fv_barcode','Mutasi'])
    return df

# @timed
def df__mutasi_filt_art(input_param):
    input_value = input_param['input_date']
    alokator = input_param['alokator']
    artikel=input_param['artikel']
    query = f"""
        SELECT a.fn_whconsiid, fv_barcode, sum(fn_stock * -1) Mutasi FROM 
        (SELECT a.fn_whconsiid, a.fv_artsizecode, fn_stock, fv_barcode from gis_local.trxstx_pb25 a
        left join  gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        left join customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        where 1=1 and fv_barcode in ('{artikel}') and fv_stat="mutasi"
        and fd_tgltrx <= '{input_value}'

        union all  
        SELECT a.fn_whconsiid, a.fv_artsizecode, fn_stock, fv_barcode from gis_local.trxstx_pb25 a
        left join  gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        left join customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        where 1=1 and fv_barcode in ('{artikel}') and fv_stat="mutasi"
        and fd_tgltrx <= '{input_value}') a
        
        group by fn_whconsiid, fv_barcode
        """
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    # df = pd.read_sql(query, engine_imp)
    if not df.empty:
        df['Mutasi']=df['Mutasi'].astype(int)
    else: df=pd.DataFrame(columns=['fn_whconsiid','fv_barcode','Mutasi'])
    return df

# @timed
def df__mutasi_between(input_param):
    year=2025
    where=add_where(input_param,cus='', art='')
    if year is None: year = datetime.now().year 
    times='this_y'
    if times == 'this_y':
        start_str = str(year) + (input_param['start'])[4:]
        ends_str = str(year) + (input_param['end'])[4:]
    else: 
        start_str = str(year) + (input_param['start_last'])[4:]
        ends_str = str(year) + (input_param['end_last'])[4:]
    whconsi_list=where['whconsi']
    divisi=where['divisi']
    cat=where['cat']
    artikel=where['artikel']
    ornal=where['ornal']
    if any([divisi, cat, ornal, artikel]):  
        # ada salah satu yang terisi
        df_art = df_artikel_pb_(
                    'fv_barcode',
                    distinct=True,
                    where=f'{cat} {artikel} {ornal} {divisi}'
                )
        artikel = df_art['fv_barcode'].dropna().unique().tolist()
        list_art = ",".join([f"'{x}'" for x in artikel])
        list_art = f'and fv_barcode in ({list_art})'
    else:
        # semuanya kosong
        list_art = ''
    query = f"""
    SELECT  fn_whconsiid, fv_artsizecode, sum(fn_qtykirim)*-1 as mutasi  FROM gis_db.t_mutasidtl a 
    inner join gis_db.t_mutasimst b on a.fv_nomutasi=b.fv_nomutasi 
    left join gis_db.article_tb c on a.fn_articleid=c.fn_articleid
    where b.fc_status <> "F" and fd_tgltransaksi between '{ends_str}' and '{start_str}' {whconsi_list} {list_art}
    group by fn_whconsiid, fv_artsizecode
    """
    # select a.fn_whconsiid, a.fv_artsizecode, sum(fn_qtykirim) Mutasi FROM gis_local.dt_mutasi_2025 a
    # left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
    # left join customers_pb c on a.fn_whconsiid=c.fn_whconsiid
    # WHERE fd_tgltransaksi between '{ends_str}' and '{start_str}' {whconsi_list} {cat} {artikel} {ornal} {divisi}
    # group by fv_toko, fv_barcode
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbsave))
    # df = pd.read_sql(query, engine_imp)
    return df

# @timed
def df__obdo(input_param):
    alokator = input_param['alokator']
    store_list = input_param['store_list']
    if store_list:
        placeholders = ", ".join([f"'{s}'" for s in store_list])
    df_cus = df__customers_pb_n(placeholders)
    whconsi_list=df_cus['fn_whconsiid'].tolist()
    whconsi_list =  ", ".join([f"'{s}'" for s in whconsi_list])

    query = f"""
        SELECT a.fn_whconsiid, fv_barcode, 
        fd_ob, 
        sum(fn_orderpersize) qty_ob, 
        fd_do, 
        sum(fn_qtyterkirim) qty_do 
        
        FROM gis_local.dt_OB_DO a 
        left join  gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        where 1=1 and  a.fn_whconsiid in ({whconsi_list})
        group by fn_whconsiid, fv_barcode
        """
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    # df = pd.read_sql(query, engine_imp)
    return df

# @timed
def change_cusid_to_whconsiid(df,df_cus):
                df=pd.merge(df,df_cus,how='left',on='fn_cusid')
                df['fn_cusid']=df['fn_whconsiid']
                df=df.drop(columns='fn_whconsiid')
                df=df.rename(columns={'fn_cusid':'fn_whconsiid'})
                return df

# @timed
def df_ostdo_(input_param=None,col='fv_barcode',df=None,col_df="",queri=False):
    df_cus = df__customers_pb_('fn_cusid,fn_whconsiid')
    fn_whcon=get_akses_data_toko(input_param,output='fn_cusid',cek_user_info=False)
    store_list = input_param['store_list']
    if store_list:
        store_list = input_param['store_list']
        if store_list: placeholders = ", ".join([f"'{s}'" for s in store_list])
        df_cus = df__customers_pb_n(placeholders)
        cus_list= df_cus['fn_cusid'].tolist()
        cus_list =  ", ".join([f"'{s}'" for s in cus_list])
        qstore_list= f'and fn_cusid in ({cus_list})'
    elif fn_whcon:
        qstore_list= f'and fn_cusid in ({fn_whcon})'
    else:
        qstore_list=''
        df_cus = df__customers_pb_('fn_cusid,fn_whconsiid')
    list_artsize=''
    if input_param['artikel']:
        artikel=input_param['artikel']
        df_art=df_artikel_pb_('fv_artsizecode',distinct=True,where=f'and fv_barcode="{artikel}"',queri=False)
        list_artsize=list_to_sql_in(df_art,'fv_artsizecode')
        list_artsize=f'and fv_artsizecode in ({list_artsize})'
    if df is not None and not df.empty:
        list_artsize = list_to_sql_in(df, col_df)
        if list_artsize:
            list_artsize = f'and fv_artsizecode in ({list_artsize})'
    query_1= f'''SELECT 
        c.fn_cusid ,fv_artsizecode, sum(fn_qty) OstDO
        FROM gis_db.dodtl_tb a 
        inner join gis_db.domst_tb b on a.fc_nodoc=b.fc_nodoc
        inner join gis_db.t_ordermst c on b.fc_noreff=fv_noorder
        left join gis_db.t_receivingmst d on b.fc_nodoc=d.fc_nosj
        where  fv_noreceiving is null {qstore_list} {list_artsize} and b.fc_status="F" 
        group by fn_cusid, fv_artsizecode, fd_date 
    '''
    if queri==True: print(query_1)
    results_1 = DatabaseConnector.execute_raw_sql(query_1, using=dbsave)
    df = pd.DataFrame(results_1)
    

    if not df.empty:
        df=change_cusid_to_whconsiid(df, df_cus)
        artsize = df['fv_artsizecode'].tolist()
        # ubah list size jadi format SQL
        list_artsize = ",".join([f"'{x}'" for x in artsize])
        if col == 'fv_artsizecode': 
            query = f'''
                    SELECT fv_artsizecode,fv_barcode from artikel_pb
                    where fv_artsizecode in ({list_artsize})
                    '''
        else:
            query = f'''
                    SELECT fv_artsizecode,{col} from artikel_pb
                    where fv_artsizecode in ({list_artsize})
                    '''
        # df_art = pd.read_sql(query, engine_imp)
        results = DatabaseConnector.execute_raw_sql(query)
        df_art = pd.DataFrame(results)
        df=pd.merge(df, df_art, on='fv_artsizecode', how='left')
        if col == 'fv_artsizecode': return df
        df=df.groupby(['fn_whconsiid',col], as_index=False)['OstDO'].sum()
        df=pd.merge(df, df_art, on=col, how='left').drop_duplicates(col)
        df=df.drop(columns='fv_artsizecode')
        return df
    else:
        if col == 'fv_artsizecode':
                return pd.DataFrame(columns=['fn_whconsiid', 'fv_artsizecode', 'OstDO','fv_barcode'])
        return pd.DataFrame(columns=['fn_whconsiid', col, 'OstDO'])

# @timed
def df_ostob_(input_param=None,col='fv_barcode',df=None,col_df="",queri=False):
    df_cus = df__customers_pb_('fn_cusid,fn_whconsiid')
    fn_whcon=get_akses_data_toko(input_param,output='fn_cusid',cek_user_info=False)
    store_list = input_param['store_list']
    if store_list:
        store_list = input_param['store_list']
        if store_list: placeholders = ", ".join([f"'{s}'" for s in store_list])
        df_cus = df__customers_pb_n(placeholders)
        cus_list= df_cus['fn_cusid'].tolist()
        cus_list =  ", ".join([f"'{s}'" for s in cus_list])
        qstore_list= f'and fn_cusid in ({cus_list})'
    elif fn_whcon:
        qstore_list= f'and fn_cusid in ({fn_whcon})'
    else:
        qstore_list=''
        df_cus = df__customers_pb_('fn_cusid,fn_whconsiid')
    list_artsize=''
    if input_param['table']=='bedah_artikel':
        artikel=input_param['artikel']
        df_art=df_artikel_pb_('fv_artsizecode',distinct=True,where=f'and fv_barcode="{artikel}"')
        list_artsize=list_to_sql_in(df_art,'fv_artsizecode')
        list_artsize=f'and fv_artsizecode in ({list_artsize})'
    if df is not None and not df.empty:
        list_artsize = list_to_sql_in(df, col_df)
        if list_artsize:
            list_artsize = f'and fv_artsizecode in ({list_artsize})'
    query_1= f'''SELECT distinct  
        fn_cusid, fv_artsizecode, sum(fn_orderpersize) OstOB FROM gis_db.t_orderdtl_goods_tmp a 
        inner join gis_db.t_ordermst b on a.fv_noorder=b.fv_noorder
        left join gis_db.domst_tb c on b.fv_noorder=c.fc_noreff
        where fc_nodoc is null {qstore_list}{list_artsize}
        group by fn_cusid, fv_artsizecode
    '''
    if queri==True: print(query_1)
    results_1 = DatabaseConnector.execute_raw_sql(query_1, using=dbsave)
    df = pd.DataFrame(results_1)
    

    if not df.empty:
        df=change_cusid_to_whconsiid(df, df_cus)
        artsize = df['fv_artsizecode'].tolist()
        # ubah list size jadi format SQL
        list_artsize = ",".join([f"'{x}'" for x in artsize])
        if col == 'fv_artsizecode': 
            query = f'''
                    SELECT fv_artsizecode,fv_barcode from artikel_pb
                    where fv_artsizecode in ({list_artsize})
                    '''
        else:
            query = f'''
                    SELECT fv_artsizecode,{col} from artikel_pb
                    where fv_artsizecode in ({list_artsize})
                    '''
        # df_art = pd.read_sql(query, engine_imp)
        results = DatabaseConnector.execute_raw_sql(query)
        df_art = pd.DataFrame(results)
        df=pd.merge(df, df_art, on='fv_artsizecode', how='left')
        if col == 'fv_artsizecode': return df
        df=df.groupby(['fn_whconsiid',col], as_index=False)['OstOB'].sum()
        df=pd.merge(df, df_art, on=col, how='left').drop_duplicates(col)
        df=df.drop(columns='fv_artsizecode')
        
        return df
    else:
        if col == 'fv_artsizecode':
                return pd.DataFrame(columns=['fn_whconsiid', 'fv_artsizecode', 'OstOB','fv_barcode'])
        return pd.DataFrame(columns=['fn_whconsiid', col, 'OstOB'])

# @timed
def df_ostdm_(input_param=None,col='fv_barcode',df=None,col_df="",queri=False):
    df_cus = df__customers_pb_('fn_cusid,fn_whconsiid')
    fn_whcon=get_akses_data_toko(input_param,output='fn_whconsiid',cek_user_info=False)
    store_list = input_param['store_list']
    if store_list:
        store_list = input_param['store_list']
        if store_list: placeholders = ", ".join([f"'{s}'" for s in store_list])
        df_cus = df__customers_pb_n(placeholders)
        cus_list= df_cus['fn_cusid'].tolist()
        cus_list =  ", ".join([f"'{s}'" for s in cus_list])
        whconsi_list=df_cus['fn_whconsiid'].tolist()
        whconsi_list =  ", ".join([f"'{s}'" for s in whconsi_list])
        whconsi_list = f'and fn_whconsiid2 in ({whconsi_list})'
    elif fn_whcon:
        whconsi_list= f'and fn_whconsiid2 in ({fn_whcon})'
    else:
        whconsi_list=''
        df_cus = df__customers_pb_('fn_cusid,fn_whconsiid')
    list_artsize=''
    if input_param['artikel']:
        artikel=input_param['artikel']
        df_art=df_artikel_pb_('fv_artsizecode',distinct=True,where=f'and fv_barcode="{artikel}"',queri=False)
        list_artsize=list_to_sql_in(df_art,'fv_artsizecode')
        list_artsize=f'and fv_artsizecode in ({list_artsize})'
    if df is not None and not df.empty:
        list_artsize = list_to_sql_in(df, col_df)
        if list_artsize:
            list_artsize = f'and fv_artsizecode in ({list_artsize})'
    query_1= f'''
        SELECT 
            fn_whconsiid2 fn_whconsiid,fv_artsizecode, sum(fn_qty) OstDM
            FROM gis_db.dodtl_tb a 
            inner join gis_db.domst_tb b on a.fc_nodoc=b.fc_nodoc
            inner join gis_db.t_mutasimst c on b.fc_noreff=c.fv_nomutasi
            left join gis_db.t_receivingmst d on b.fc_nodoc=d.fc_nosj
            where  fv_noreceiving is null {whconsi_list} {list_artsize}
            and b.fc_status="F" 
            group by fn_whconsiid2, fv_artsizecode, fd_date 
        '''
    if queri==True: print(query_1)
    # df = pd.read_sql(query, engine_write)
    results = DatabaseConnector.execute_raw_sql(query_1, using=dbsave)
    df = pd.DataFrame(results)
    if not df.empty:
        artsize = df['fv_artsizecode'].tolist()
        # ubah list size jadi format SQL
        list_artsize = ",".join([f"'{x}'" for x in artsize])
        if col == 'fv_artsizecode': 
            query = f'''
                    SELECT fv_artsizecode,fv_barcode from artikel_pb
                    where fv_artsizecode in ({list_artsize})
                    '''
        else:
            query = f'''
                    SELECT fv_artsizecode,{col} from artikel_pb
                    where fv_artsizecode in ({list_artsize})
                    '''
        # df_art = pd.read_sql(query, engine_imp)
        results = DatabaseConnector.execute_raw_sql(query)
        df_art = pd.DataFrame(results)
        df=pd.merge(df, df_art, on='fv_artsizecode', how='left')
        if col == 'fv_artsizecode':return df
        df=df.groupby(['fn_whconsiid',col,], as_index=False)[['OstDM']].sum()
        df=pd.merge(df, df_art, on=col, how='left').drop_duplicates(col)
        df=df.drop(columns='fv_artsizecode')
        return df
    else:
        if col == 'fv_artsizecode':
            return pd.DataFrame(columns=['fn_whconsiid', 'fv_artsizecode', 'OstDM','fv_barcode'])
        return pd.DataFrame(columns=['fn_whconsiid', col, 'OstDM'])

# @timed
def df__art_stock_wh(col='fv_barcode,',gb='fv_barcode',type_show='all',where=""):
    if type_show == 'all':
        whid=''
    else: whid='and a.fn_whtypeid in (6,2)'
    query = f"""
        SELECT distinct
            {col} sum(fn_qty) fn_qty
        FROM gis_db.stockarticle_tb a 
        LEFT JOIN gis_db.articlesize_tb b ON a.fv_artsizecode = b.fv_artsizecode
        LEFT JOIN gis_db.article_tb c  ON b.fn_articleid = c.fn_articleid
        LEFT JOIN gis_db.warehouse_tb d  ON a.fn_whid = d.fn_whid
            WHERE 1=1 {whid} {where} AND fn_qty > 0 and fv_barcode is not null
        GROUP BY {gb}
    """
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    return pd.DataFrame(results)

# @timed
def df__stock_wh(input_param,df=None,type_show='all',list_from='fv_barcode'):
    where=add_where(input_param,cus='', art='')
    cat=where['cat']
    if df is None and cat=='': 
        artsize=df__select_articlesize_from_list(input_param, list_art=input_param['artikel'])
    elif cat and df is None:
        df_art=df_artikel_pb_('fv_artsizecode',distinct=True,where=cat,queri=False)
        artsize = df_art['fv_artsizecode'].dropna().tolist()
    elif df is not None and list_from == 'fv_artsizecode':
        artsize = df['fv_artsizecode'].dropna().tolist()
    else: 
        list_barcode = df['fv_barcode'].dropna().tolist()
        if not list_barcode:
            return pd.DataFrame(columns=['fv_whtypename','fv_artsizecode','fn_qty'])

        # cari semua size dari list barcode
        artsize = df__select_articlesize_from_list(input_param, list_art=list_barcode)
        if not artsize:
            return pd.DataFrame(columns=['fv_whtypename','fv_artsizecode','fn_qty'])
        # ubah list size jadi format SQL
    list_artsize = ",".join([f"'{x}'" for x in artsize])
    list_artsize = f'and fv_artsizecode IN ({list_artsize})'
    if type_show == 'all':
        whid=''
    elif type_show == 'pst':
        whid='and a.fn_whtypeid in (6)'
    elif type_show == 'konsi':
        whid='and a.fn_whtypeid in (2)'
    else: whid='and a.fn_whtypeid in (6,2)'
    query = f"""
        SELECT 
            fv_whtypename, fv_artsizecode, fn_qty 
        FROM gis_db.stockarticle_tb a 
        LEFT JOIN gis_db.warehousetype_tb b 
            ON a.fn_whtypeid = b.fn_whtypeid
        WHERE 1=1 {list_artsize}
          AND a.fn_whtypeid not in (0,1) {whid}
          AND fn_qty > 0
    """
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    return pd.DataFrame(results)

# @timed
def df__art_wh():
    query = f"""
        SELECT distinct fv_barcode, sum(fn_qty) stocks
        FROM gis_db.stockarticle_tb a 
        LEFT JOIN gis_db.articlesize_tb b 
            ON a.fv_artsizecode = b.fv_artsizecode
		LEFT JOIN gis_db.article_tb c 
            ON b.fn_articleid = c.fn_articleid
        WHERE 1=1 
          AND a.fn_whtypeid not in (0,1) 
          AND fn_qty > 0
          
        group by fv_barcode
		having sum(fn_qty) > 0
    """
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    return pd.DataFrame(results)

# @timed
def df__art_fstock_wh(type_show='all'):
    if type_show == 'all':
        whid=''
    else: whid='and fn_whid in (6,2)'
    query= f'''
    SELECT distinct
            fv_barcode, sum(fn_qty) fn_qty FROM (
        SELECT 
            distinct fn_whid, fv_artsizecode, sum(fn_orderingpersize * -1) fn_qty
            FROM gis_db.t_orderdtl_goods_tmp_pos 
            where fv_noorder not like 'OB%%' and fv_noorder not like 'SO%%'  
            and fc_status='T' and fn_whid not in (0,1) {whid}
            group by fn_whid, fv_artsizecode
	union all 
        SELECT 
            distinct fn_whid, fv_artsizecode, sum(fn_orderingpersize * -1) fn_qty FROM gis_db.t_orderdtl_goods_tmp
            where fc_status ="T" and fn_whid not in (0,1) {whid}
            group by fn_whid, fv_artsizecode) a 
    left join gis_db.warehousetype_tb b on a.fn_whid=b.fn_whtypeid
    LEFT JOIN gis_db.articlesize_tb f ON a.fv_artsizecode = f.fv_artsizecode
    LEFT JOIN gis_db.article_tb c  ON f.fn_articleid = c.fn_articleid
    GROUP BY fv_barcode
        '''
    # # printol(query)
    # df = pd.read_sql(query, engine_write)
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    return pd.DataFrame(results)

# @timed
def df__fstock_wh(input_param,df=None, type_show='all'):# ambil list barcode unik dari df
    if df is None: 
        artsize=df__select_articlesize_from_list(input_param, list_art=input_param['artikel'])
    else: 
        list_barcode = df['fv_barcode'].tolist()
        if not list_barcode:
            return pd.DataFrame(columns=['fv_whtypename','fv_artsizecode','fn_qty'])

        # cari semua size dari list barcode
        artsize = df__select_articlesize_from_list(input_param, list_art=list_barcode)
        if not artsize:
            return pd.DataFrame(columns=['fv_whtypename','fv_artsizecode','fn_qty'])

        # ubah list size jadi format SQL
    list_artsize = ",".join([f"'{x}'" for x in artsize])
    if type_show == 'all':
        whid=''
    else: whid='and fn_whid in (6,2)'
    query= f'''
    SELECT 
    distinct fv_whtypename, fv_artsizecode, fn_qty FROM (
        SELECT 
            distinct fn_whid, fv_artsizecode, sum(fn_orderingpersize * -1) fn_qty
            FROM gis_db.t_orderdtl_goods_tmp_pos 
            where fv_noorder not like 'OB%%' and fv_noorder not like 'SO%%'  
            and fc_status='T' and fv_artsizecode IN ({list_artsize}) and fn_whid not in (0,1) {whid}
            group by fn_whid, fv_artsizecode
	union all 
        SELECT 
            distinct fn_whid, fv_artsizecode, sum(fn_orderingpersize * -1) fn_qty FROM gis_db.t_orderdtl_goods_tmp
            where fc_status ="T" and fv_artsizecode IN ({list_artsize}) and fn_whid not in (0,1) {whid}
            group by fn_whid, fv_artsizecode) a 
    left join gis_db.warehousetype_tb b on a.fn_whid=b.fn_whtypeid
        '''
    # # printol(query)
    # df = pd.read_sql(query, engine_write)
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    df = pd.DataFrame(results)
    return df

# @timed
def df__art_onbook_stock(input_param,df=None, type_show='all',list_from ='fv_barcode', set_minus=True):# ambil list barcode unik dari df
    if df is None: 
        artsize=df__select_articlesize_from_list(input_param, list_art=input_param['artikel'])
    elif df is not None and list_from == 'fv_artsizecode':
        artsize = df['fv_artsizecode'].dropna().tolist()
    else: 
        list_barcode = df['fv_barcode'].tolist()
        if not list_barcode:
            return pd.DataFrame(columns=['fv_whtypename','fv_artsizecode','fn_qty'])

        # cari semua size dari list barcode
        artsize = df__select_articlesize_from_list(input_param, list_art=list_barcode)
        if not artsize:
            return pd.DataFrame(columns=['fv_whtypename','fv_artsizecode','fn_qty'])

        # ubah list size jadi format SQL
    list_artsize = ",".join([f"'{x}'" for x in artsize])
    if type_show == 'all':
        whid=''
    elif type_show == 'pst':
        whid='and fn_whid in (6)'
    elif type_show == 'konsi':
        whid='and fn_whid in (2)'
    else: whid='and fn_whid in (6,2)'
    if set_minus is True: set_minus='* -1'
    else: set_minus=''
    query= f'''
    SELECT 
    distinct fv_whtypename, fv_artsizecode, fn_qty FROM (
        SELECT 
            distinct fn_whid, fv_artsizecode, sum(fn_orderingpersize {set_minus}) fn_qty
            FROM gis_db.t_orderdtl_goods_tmp_pos 
            where fv_noorder not like 'OB%%' and fv_noorder not like 'SO%%'  
            and fc_status='T' and fv_artsizecode IN ({list_artsize}) and fn_whid not in (0,1) {whid}
            group by fn_whid, fv_artsizecode
	union all 
        SELECT 
            distinct fn_whid, fv_artsizecode, sum(fn_orderingpersize {set_minus}) fn_qty FROM gis_db.t_orderdtl_goods_tmp
            where fc_status ="T" and fv_artsizecode IN ({list_artsize}) and fn_whid not in (0,1) {whid}
            group by fn_whid, fv_artsizecode) a 
    left join gis_db.warehousetype_tb b on a.fn_whid=b.fn_whtypeid
        '''
    # # printol(query)
    # df = pd.read_sql(query, engine_write)
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    df = pd.DataFrame(results)
    return df

# @timed
def df__onbook_stock(input_param,df=None, 
                     type_show='all',
                     subid=False, 
                     fn_nodraf=False, 
                     fn_whconsiid=False,
                     list_from=None, set_minus=True):
    # ambil list barcode unik dari df
    where=add_where(input_param,cus='', art='')
    whconsi_list=where['whconsi']
    whconsi_list=whconsi_list.replace('fn_whconsiid','fn_whconsiid2')
    if type_show == 'all':whid=''
    elif type_show == 'pst':
        whid='and fn_whconsiid in (0)'
    elif type_show == 'konsi':
        whid='and fn_whconsiid in (9999)'
    else: whid='and fn_whconsiid in (0,9999)'# on proc#
    
    if subid is False: subid=''
    else: subid=f"and fn_userid = {input_param['subid']}"
    
    if fn_nodraf is False: fn_nodraf=''
    else: fn_nodraf=f', fv_nodraft '

    if fn_whconsiid is False: 
        fn_whconsiid=''
        whconsi_list=''
    else: fn_whconsiid=f', fn_whconsiid2  fn_whconsiid'

    if input_param['divisi'] is not None and input_param['divisi']!='':
        divisi = input_param['divisi']
        divisi = f'and fv_divname="{divisi}"'
    else: divisi = ''

    if input_param['ornal'] is not None and input_param['ornal']!='':
        ornal = input_param['ornal']
        ornal = f'and fv_ornal="{ornal}"'
    else: ornal = ''

    if df is None : 
        artsize=df__select_articlesize_from_list(input_param)
        list_artsize = ",".join([f"'{x}'" for x in artsize])
        list_artsize = f'and fv_artsizecode IN ({list_artsize})'
    elif isinstance(df, str) and df == 'self':
        query= f'''
            SELECT 
                fv_artsizecode
                FROM gis_local.t_draftdtl
                where fc_status='X' and (fv_nodraft like "BK%%" or fv_nodraft like "OB%%"){whid} {subid} {whconsi_list}
            '''
        
        results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
        df = pd.DataFrame(results)
        if df.empty:
            return pd.DataFrame(columns=['fv_whtypename','fv_artsizecode','fn_qty','fn_whconsiid'])
        artsize=[x for x in df['fv_artsizecode'].tolist() if x]
        list_artsize =  ",".join([f"'{x}'" for x in artsize]) 
        
        df=df_artikel_pb_('fv_artsizecode',distinct=True,where=f'''
        and fv_artsizecode IN ({list_artsize}) {divisi} {ornal} 
        ''')
        if df.empty:
            return pd.DataFrame(columns=['fv_whtypename','fv_artsizecode','fn_qty','fn_whconsiid'])
        list_artsize = df['fv_artsizecode'].tolist()
        list_artsize = ",".join([f"'{x}'" for x in list_artsize])
        list_artsize = f'and fv_artsizecode IN ({list_artsize})'
    else: 
        list_barcode = df['fv_barcode'].tolist()
        if not list_barcode:
            return pd.DataFrame(columns=['fv_whtypename','fv_artsizecode','fn_qty'])

        # cari semua size dari list barcode
        artsize = df__select_articlesize_from_list(input_param, list_art=list_barcode)
        if not artsize:
            return pd.DataFrame(columns=['fv_whtypename','fv_artsizecode','fn_qty'])
        list_artsize = ",".join([f"'{x}'" for x in artsize])
        list_artsize = f'and fv_artsizecode IN ({list_artsize})'
        if list_from is not None and list_from!='':
            list_artsize=",".join([f"'{x}'" for x in list_barcode])
            list_artsize = f'and {list_from} IN ({list_artsize})'
    if set_minus is True: set_minus='* -1'
    else: set_minus=''
    query= f'''
        SELECT 
            fn_whconsiid fv_whtypename, fv_artsizecode{fn_nodraf}, sum(fn_on_book {set_minus}) fn_qty {fn_whconsiid}
            FROM gis_local.t_draftdtl
            where fc_status='X' {list_artsize} {whid} {subid} {whconsi_list}
            group by fn_whconsiid, fv_artsizecode 
        '''
    # df = pd.read_sql(query, engine_write)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    if df.empty:
        return df
    df['fv_whtypename'] = np.where(
                        df['fv_whtypename'] == 0, 'PUSAT',
                        np.where(df['fv_whtypename'] == 9999, 'KONSINETTO', df['fv_whtypename']))
    return df

# @timed
def df__req_retur(input_param):
    artikel=input_param['artikel']
    if artikel is not None and artikel!='':
        list_artsize=df__select_articlesize_from_artikel(input_param)
        list_artsize = ", ".join([f"'{x}'" for x in list_artsize])
        list_artsize=f'and fv_artsizecode in ({list_artsize})'
        qstore_list=''
    else:
        # list_artsize=''
        store_list = input_param['store_list']
        if store_list:
            placeholders = ", ".join([f"'{s}'" for s in store_list])
        df_cus = df__customers_pb_n(placeholders)
        cus_list= df_cus['fn_cusid'].tolist()
        cus_list =  ", ".join([f"'{s}'" for s in cus_list])
        whconsi_list=df_cus['fn_whconsiid'].tolist()
        whconsi_list =  ", ".join([f"'{s}'" for s in whconsi_list])
        qstore_list= f'and fn_whconsiid in ({whconsi_list})'
        list_artsize=''
    query = f'''
        SELECT fn_whconsiid, (CAST(fn_qtykirim AS SIGNED) * -1) as stocks, fv_artsizecode from t_returdtl_pre t
        LEFT JOIN t_returmst_pre m on t.fv_noretur = m.fv_noretur
        WHERE t.fc_status in ('T') {qstore_list} {list_artsize}

        '''
    # {list_artsize}
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    df = pd.DataFrame(results)
    return df

# @timed
def df__req_mutasi(input_param):
    artikel=input_param['artikel']
    if artikel is not None and artikel!='':
        list_artsize=df__select_articlesize_from_artikel(input_param)
        list_artsize = ", ".join([f"'{x}'" for x in list_artsize])
        list_artsize=f'and fv_artsizecode in ({list_artsize})'
        qstore_list=''
    else:
        store_list = input_param['store_list']
        if store_list:
            placeholders = ", ".join([f"'{s}'" for s in store_list])
        df_cus = df__customers_pb_n(placeholders)
        cus_list= df_cus['fn_cusid'].tolist()
        cus_list =  ", ".join([f"'{s}'" for s in cus_list])
        whconsi_list=df_cus['fn_whconsiid'].tolist()
        whconsi_list =  ", ".join([f"'{s}'" for s in whconsi_list])
        qstore_list= f'and fn_whconsiid in ({whconsi_list})'
    query = f'''
        SELECT fn_whconsiid, (CAST(fn_qtykirim AS SIGNED) * -1) as stocks, fv_artsizecode from t_mutasidtl_pre t
        LEFT JOIN t_mutasimst_pre m on t.fv_nomutasi = m.fv_nomutasi    
        WHERE t.fc_status in ('T') {qstore_list}
        '''
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    df = pd.DataFrame(results)
    return df

# @timed
def df__req_retur_sum(input_param, col='',queri=False):
    start_str = (input_param['start'])
    ends_str = (input_param['end'])
    store_list = input_param['store_list']
    # =========================
    # Filter store (opsional)
    # =========================
    qstore_list = ''
    if store_list:
        placeholders = ", ".join(f"'{s}'" for s in store_list)
        df_cus = df__customers_pb_n(placeholders)
        if df_cus.empty:
            # tidak ada store match → return kosong
            return pd.DataFrame(
                columns=['fn_whconsiid', 'stocks', 'fv_artsizecode']
            )
        whconsi_list = ", ".join(
            f"'{x}'" for x in df_cus['fn_whconsiid'].tolist()
        )
        qstore_list = f'AND fn_whconsiid IN ({whconsi_list})'
    list_artsize=''
    if input_param['artikel']:
        artikel={input_param['artikel']}
        artikel = ", ".join(f"'{x}'" for x in artikel)
        df_art=df_artikel_pb_('fv_artsizecode',distinct=True,where=f'and fv_barcode in ({artikel})')
        list_artsize=list_to_sql_in(df_art,'fv_artsizecode')
        list_artsize=f'and fv_artsizecode in ({list_artsize})'
    # =========================
    # Query utama
    # =========================
    name_col=f''
    query = f'''
        SELECT fn_whconsiid, (CAST(fn_qtykirim AS SIGNED) * -1) as stocks{name_col}, fv_artsizecode {col} from t_returdtl_pre t
        LEFT JOIN t_returmst_pre m on t.fv_noretur = m.fv_noretur
        WHERE t.fc_status in ('T') {qstore_list} {list_artsize} and STR_TO_DATE(fd_tglretur, '%%Y-%%m-%%d') between '{ends_str}' and '{start_str}'
    '''
    if queri==True: print(query)
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    df = pd.DataFrame(results)
    return df

# @timed
def df__req_mutasi_sum(input_param, in_out='out', col='',queri=False):
    start_str = (input_param['start']) + ' 23:59:59'
    ends_str = (input_param['end']) + ' 00:00:00'
    store_list = input_param['store_list']
    if store_list:
        placeholders = ", ".join([f"'{s}'" for s in store_list])
    df_cus = df__customers_pb_n(placeholders)
    cus_list= df_cus['fn_cusid'].tolist()
    cus_list =  ", ".join([f"'{s}'" for s in cus_list])
    whconsi_list=df_cus['fn_whconsiid'].tolist()
    whconsi_list =  ", ".join([f"'{s}'" for s in whconsi_list])
    if in_out== 'out': 
        whconsi='fn_whconsiid'
        multiple='-1'
    else: 
        whconsi='fn_whconsiid2'
        multiple='1'
    qstore_list= f'and {whconsi} in ({whconsi_list})'
    name_col=f''
    query = f'''
        SELECT {whconsi}, (CAST(fn_qtykirim AS SIGNED) * {multiple}) as stocks{name_col}, fv_artsizecode {col} from t_mutasidtl_pre t
        LEFT JOIN t_mutasimst_pre m on t.fv_nomutasi = m.fv_nomutasi    
        WHERE t.fc_status in ('T') {qstore_list} and fd_tglmutasi between '{ends_str}' and '{start_str}'
        '''
    if queri==True: print(query)
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    df = pd.DataFrame(results)
    return df

# @timed
def df__req_mutasi_sum(input_param, in_out='out', col='',queri=False):
    start_str = input_param['start'] + ' 23:59:59'
    ends_str  = input_param['end']   + ' 00:00:00'
    store_list = input_param.get('store_list', [])
    # =========================
    # Tentukan kolom mutasi
    # =========================
    if in_out == 'out':
        whconsi_col = 'fn_whconsiid'
        multiple = -1
    else:
        whconsi_col = 'fn_whconsiid2'
        multiple = 1
    # =========================
    # Filter store (opsional)
    # =========================
    qstore_list = ''
    if store_list:
        placeholders = ", ".join(f"'{s}'" for s in store_list)
        df_cus = df__customers_pb_n(placeholders)
        if df_cus.empty:
            return pd.DataFrame(
                columns=[whconsi_col, 'stocks', 'fv_artsizecode']
            )
        whconsi_list = ", ".join(
            f"'{x}'" for x in df_cus['fn_whconsiid'].tolist()
        )
        qstore_list = f'AND {whconsi_col} IN ({whconsi_list})'
    list_artsize=''
    if input_param['artikel']:
        artikel=input_param['artikel']
        # artikel = ", ".join(f"'{x}'" for x in artikel)
        df_art=df_artikel_pb_('fv_artsizecode',distinct=True,where=f'and fv_barcode in ("{artikel}")',queri=False)
        list_artsize=list_to_sql_in(df_art,'fv_artsizecode')
        list_artsize=f'and t.fv_artsizecode in ({list_artsize})'
    # =========================
    # Query utama
    # =========================
    query = f'''
        SELECT
            {whconsi_col},
            (CAST(t.fn_qtykirim AS SIGNED) * {multiple}) AS stocks,
            t.fv_artsizecode {col}
        FROM t_mutasidtl_pre t
        LEFT JOIN t_mutasimst_pre m
            ON t.fv_nomutasi = m.fv_nomutasi
        WHERE
            t.fc_status = 'T' and m.fc_status = 'T'
            {qstore_list} {list_artsize}
            AND m.fd_tglmutasi
                BETWEEN '{ends_str}' AND '{start_str}'
    '''
    if queri==True: print(query)
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    return pd.DataFrame(results)

# @timed
def df__trx_freestock(input_param):
    input_now = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    input_value = datetime.strptime(input_param['input_date'], "%Y-%m-%d")

    if input_now != input_value:
        input_value += timedelta(days=1)
    input_value = input_value.strftime("%Y-%m-%d")
    trxstx = f"trxstx_pb{input_value[2:4]}"
    artikel=input_param['artikel']
    if artikel==None or artikel=='':
        store_list = input_param['store_list']
        if store_list:
            placeholders = ", ".join([f"'{s}'" for s in store_list])
        df_cus = df__customers_pb_n(placeholders)
        cus_list= df_cus['fn_cusid'].tolist()
        cus_list =  ", ".join([f"'{s}'" for s in cus_list])
        whconsi_list=df_cus['fn_whconsiid'].tolist()
        whconsi_list =  ", ".join([f"'{s}'" for s in whconsi_list])
        qstore_list= f'and b.fn_whconsiid in ({whconsi_list})'
    
    else:
        qstore_list= f'and fv_barcode in ("{artikel}")'

    main_query = f"""
    SELECT 
        stock_backdate.fn_whconsiid,
        SUM(stock) AS stocks,
        stock_backdate.fv_artsizecode, fv_stat, fd_tgltrx
    FROM (
        SELECT c.fn_whconsiid, fn_stock AS stock, c.fv_artsizecode , fv_stat, fd_tgltrx
        FROM gis_local.{trxstx} c
        LEFT JOIN artikel_pb a ON c.fv_artsizecode = a.fv_artsizecode
        LEFT JOIN customers_pb b ON c.fn_whconsiid = b.fn_whconsiid
        WHERE fd_tgltrx BETWEEN '{input_value}' AND '{input_now}' 
        and fv_stat not in ('adj_out','adj_in') {qstore_list}
        UNION ALL

        SELECT c.fn_whconsiid, fn_stock AS stock, c.fv_artsizecode , fv_stat, fd_tgltrx
        FROM gis_local.{trxstx} c
        LEFT JOIN artikel_pb a ON c.fv_artsizecode = a.fv_artsizecode
        LEFT JOIN customers_pb b ON c.fn_whconsiid = b.fn_whconsiid
        WHERE fd_tgltrx BETWEEN '{input_value}' AND '{input_now}' 
        and fv_stat in ('adj_out','adj_in') {qstore_list}
        UNION ALL

        SELECT c.fn_whconsiid, fn_stock AS stock, c.fv_artsizecode , fv_stat, fd_tgltrx
        FROM gis_local.trxrt_pb c
        LEFT JOIN artikel_pb a ON c.fv_artsizecode = a.fv_artsizecode
        LEFT JOIN customers_pb b ON c.fn_whconsiid = b.fn_whconsiid
        WHERE fd_tgltrx > '{input_value}' {qstore_list}
        
    ) stock_backdate
    GROUP BY fn_whconsiid, fv_artsizecode, fv_stat, fd_tgltrx
    """
    # # printol(main_query)
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
            df = pd.DataFrame(results)
            return df  # success, langsung return
        except Exception as e:
            print(f"[Attempt {attempt}] Query gagal: {e}")
            if attempt == max_retries:
                raise  # kalau sudah max retry, lempar error
            time.sleep(2)  # tunggu 2 detik sebelum nyoba lagi
    # results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
    # df = pd.DataFrame(results)
    # return df

# @timed
def df__penjualan(input_param,queri=False):
    store_list =input_param['store_list']
    if store_list  is not None:
        if store_list:
            placeholders = ", ".join([f"'{s}'" for s in store_list])
            df_cus = df__customers_pb_n(placeholders)
            whconsi_list = df_cus['fn_whconsiid'].tolist()
            
            if whconsi_list:
                whconsi_placeholders = ", ".join([str(s) for s in whconsi_list])
                whconsi = f"AND fn_whconsiid IN ({whconsi_placeholders})"
            else: whconsi = ""  # atau kondisi default kalau kosong
    else: whconsi = ""  # jika input_param['store_list'] None

    if input_param['divisi'] is not None and input_param['divisi']!='':
        divisi = input_param['divisi']
        divisi = f'and fv_divname="{divisi}"'
    else: divisi = ''

    if input_param['ornal'] is not None and input_param['ornal']!='':
        ornal = input_param['ornal']
        ornal = f'and a.fv_ornal="{ornal}"'
    else: ornal = ''

    this_year=(input_param['start'])[0:4]
    last_year=str(int(this_year)-1)
    main_query = f"""
    SELECT
    bulan,
    penjualan_{last_year} as penjualan_2024,
    ifnull(penjualan_{this_year},0) as penjualan_2025,
    ifnull((penjualan_{this_year} - penjualan_{last_year}),0) AS offer_under,ifnull(
    CASE
        WHEN penjualan_{last_year} = 0 THEN NULL
        ELSE ROUND(((penjualan_{this_year} - penjualan_{last_year}) / penjualan_{last_year}) * 100, 2)
    END,0) AS growth_percent
    FROM (
    SELECT
        'January' AS bulan,
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{last_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'January' {whconsi} {divisi} {ornal}) AS penjualan_{last_year},
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{this_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'January' {whconsi} {divisi} {ornal}) AS penjualan_{this_year}
    union all
        SELECT
        'February' AS bulan,
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{last_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'February' {whconsi} {divisi} {ornal}) AS penjualan_{last_year},
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{this_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'February' {whconsi} {divisi} {ornal}) AS penjualan_{this_year}
    union all
        SELECT
        'March'AS bulan,
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{last_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'March' {whconsi} {divisi} {ornal}) AS penjualan_{last_year},
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{this_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'March' {whconsi} {divisi} {ornal}) AS penjualan_{this_year}
    union all
        SELECT
        'April' AS bulan,
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{last_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'April' {whconsi} {divisi} {ornal}) AS penjualan_{last_year},
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{this_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'April' {whconsi} {divisi} {ornal}) AS penjualan_{this_year}
    union all
        SELECT
        'May' AS bulan,
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{last_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'May' {whconsi} {divisi} {ornal}) AS penjualan_{last_year},
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{this_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'May' {whconsi} {divisi} {ornal}) AS penjualan_{this_year}
    union all
        SELECT
        'June' AS bulan,
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{last_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'June' {whconsi} {divisi} {ornal}) AS penjualan_{last_year},
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{this_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'June' {whconsi} {divisi} {ornal}) AS penjualan_{this_year}
    union all
        SELECT
        'July' AS bulan,
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{last_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'July' {whconsi} {divisi} {ornal}) AS penjualan_{last_year},
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{this_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'July' {whconsi} {divisi} {ornal}) AS penjualan_{this_year}
    union all
        SELECT
        'August' AS bulan,
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{last_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'August' {whconsi} {divisi} {ornal}) AS penjualan_{last_year},
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{this_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'August' {whconsi} {divisi} {ornal}) AS penjualan_{this_year}
    union all
        SELECT
        'September' AS bulan,
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{last_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'September' {whconsi} {divisi} {ornal}) AS penjualan_{last_year},
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{this_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'September' {whconsi} {divisi} {ornal}) AS penjualan_{this_year}
    union all
        SELECT
        'October' AS bulan,
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{last_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'October' {whconsi} {divisi} {ornal}) AS penjualan_{last_year},
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{this_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'October' {whconsi} {divisi} {ornal}) AS penjualan_{this_year}
    union all
        SELECT
        'November' AS bulan,
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{last_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'November' {whconsi} {divisi} {ornal}) AS penjualan_{last_year},
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{this_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'November' {whconsi} {divisi} {ornal}) AS penjualan_{this_year}
    union all
        SELECT
        'December' AS bulan,
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{last_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'December' {whconsi} {divisi} {ornal}) AS penjualan_{last_year},
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{this_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE fc_monthname = 'December' {whconsi} {divisi} {ornal}) AS penjualan_{this_year}
    union all
        SELECT
        'Total' AS bulan,
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{last_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE 1=1 {whconsi} {divisi} {ornal}) AS penjualan_{last_year},
        (SELECT SUM(fn_totalpenjualan) FROM gis_local.pb_penjualan_{this_year} a 
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE 1=1 {whconsi} {divisi} {ornal}) AS penjualan_{this_year}
    ) AS penju;
    """
    if queri==True: print(main_query)
    results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__ornal(input_param,year=None):
    if year is None: year = datetime.now().year 
    else: year = int(year)
    store_list =input_param['store_list']
    if store_list  is not None:
        if store_list:
            placeholders = ", ".join([f"'{s}'" for s in store_list])
            df_cus = df__customers_pb_n(placeholders)
            whconsi_list = df_cus['fn_whconsiid'].tolist()
            
            if whconsi_list:
                whconsi_placeholders = ", ".join([str(s) for s in whconsi_list])
                whconsi = f"AND fn_whconsiid IN ({whconsi_placeholders})"
            else: whconsi = ""  # atau kondisi default kalau kosong
    else: whconsi = ""  # jika input_param['store_list'] None
    if input_param['divisi'] is not None:
        divisi = input_param['divisi']
        divisi = f'and fv_divname="{divisi}"'
    else: divisi = ''
    bulan=input_param['bulan']
    main_=generate_main_query(bulan)
    sales_thru=f'sales_thru_pb_{str(year)[-2:]}'
    main_query = f"""
    SELECT 
    sales_thru.ornal,
    sales_thru.stock_awal,
    sales_thru.total_stock,
    sales_thru.sales,
    ROUND(100.0 * sales_thru.sales / NULLIF(total_data.sales, 0), 2) AS sales_kontribusi,
    sales_thru.stock_akhir,
    ROUND(100.0 * sales_thru.stock_akhir / NULLIF(total_data.stock_akhir, 0), 2) AS stock_kontribusi
    FROM
    (
        SELECT ornal, 
        stock_awal,
        (stock_akhir + sales) total_stock,
        sales,
        stock_akhir
        from (select "Normal" as ornal, {main_} FROM gis_local.{sales_thru} a
        left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        WHERE fv_ornal='Normal' {whconsi} {divisi}
        union all
        select "Obral" as ornal, {main_} FROM gis_local.{sales_thru} a
        left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        WHERE fv_ornal='Obral' {whconsi} {divisi}
        union all
        select "Total" as ornal,{main_} FROM gis_local.{sales_thru} a
        left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
        WHERE 1=1 {whconsi} {divisi}) sub 
        group by ornal) sales_thru,

        (
            SELECT 
                {main_}
            FROM gis_local.{sales_thru} a
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
            WHERE 1=1 {whconsi} {divisi}
        ) total_data
    """
    results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__sum_kategori(input_param,year=None):
    if year is None: year = datetime.now().year 
    else: year = int(year)
    store_list =input_param['store_list']
    if store_list  is not None:
        if store_list:
            placeholders = ", ".join([f"'{s}'" for s in store_list])
            df_cus = df__customers_pb_n(placeholders)
            whconsi_list = df_cus['fn_whconsiid'].tolist()
            
            if whconsi_list:
                whconsi_placeholders = ", ".join([str(s) for s in whconsi_list])
                whconsi = f"AND fn_whconsiid IN ({whconsi_placeholders})"
            else: whconsi = ""  # atau kondisi default kalau kosong
    else: whconsi = ""  # jika input_param['store_list'] None
    if input_param['divisi'] is not None:
        divisi = input_param['divisi']
        divisi = f'and fv_divname="{divisi}"'
    else: divisi = ''
    bulan=input_param['bulan']
    main_=generate_main_query(bulan)
    sales_thru=f'sales_thru_pb_{str(year)[-2:]}'
    main_query = f"""
    select
    sales_thru.fv_catname kategory,
    sales_thru.stock_awal,
    sales_thru.total_stock,
    sales_thru.sales,
    ROUND(100.0 * sales_thru.sales / NULLIF(total_data.sales, 0), 2) AS sales_kontribusi,
        sales_thru.stock_akhir,
    ROUND(100.0 * sales_thru.stock_akhir / NULLIF(total_data.stock_akhir, 0), 2) AS stock_kontribusi
    from (

        select fv_catname , 
        CAST(stock_awal AS SIGNED) AS stock_awal, 
        CAST(stock_akhir + sales AS SIGNED) AS total_stock,
        CAST(sales AS SIGNED) AS sales, 
        CAST(stock_akhir AS SIGNED) AS stock_akhir from (

        select fv_catname, {main_}
            FROM gis_local.{sales_thru} a
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
            WHERE 1=1 {whconsi} {divisi}
            group by fv_catname
        union all

        select "TOTAL" as fv_catname, {main_}
            FROM gis_local.{sales_thru} a
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
            WHERE 1=1 {whconsi} {divisi} )sub
            ) sales_thru,
            (
                select fv_catname, {main_}
                FROM gis_local.{sales_thru} a
                left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
                WHERE 1=1 {whconsi} {divisi} 
            ) total_data
    """
    results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__art_stock_now(input_param, 
                         col='stock_backdate.fn_whconsiid, fv_artsizecode, fv_barcode, fv_sizename, fv_configname,',
                         gb='stock_backdate.fn_whconsiid, fv_artsizecode',
                         year=None,
                         cus='',
                         art='',
                         times='this_y'):
    where=add_where(input_param,cus=cus, art=art)
    whconsi_list=where['whconsi']
    divisi=where['divisi']
    cat=where['cat']
    artikel=where['artikel']
    ornal=where['ornal']
    if year is None: year = datetime.now().year 
    if times == 'this_y': 
        input_value = input_param['start']
    else: 
        input_value= input_param['start_last']
    sales_thru=f'sales_thru_pb_{str(year)[-2:]}'
    trxstx = f"trxstx_pb{str(year)[-2:]}"
    stock = f"fn_stock_{input_value[5:7]}"
    main_query = f"""
        SELECT {col}
            SUM(stock) AS stocks
        FROM (
            SELECT fn_whconsiid, {stock} AS stock, fv_artsizecode 
            FROM gis_local.{sales_thru}
            where 1=1 {whconsi_list} 
            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
            FROM gis_local.{trxstx} 
            WHERE fd_tgltrx BETWEEN '{str(year)[-2:]}-{input_value[5:7]}-01' AND '{str(year)[-2:]}-{input_value[5:10]}'
            {whconsi_list} 
            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
            FROM gis_local.trxrt_pb
            WHERE fd_tgltrx < '{str(year)[-2:]}-{input_value[5:10]}' {whconsi_list} 
            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
            FROM gis_local.Stock_varian_{str(year)[-2:]}
            WHERE fd_tgl BETWEEN '{str(year)[-2:]}-{input_value[5:7]}-01' AND '{str(year)[-2:]}-{input_value[5:10]}'
            {whconsi_list} 
        ) stock_backdate
        LEFT JOIN artikel_pb a ON a.fv_artsizecode = stock_backdate.fv_artsizecode
        LEFT JOIN customers_pb b ON stock_backdate.fn_whconsiid = b.fn_whconsiid
        WHERE 1=1 {cat} {artikel} {ornal} {divisi}
        GROUP BY {gb}
        """
    # Retry up to 5 times if error
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
            df = pd.DataFrame(results)
            return df  # success, langsung return
        except Exception as e:
            print(f"[Attempt {attempt}] Query gagal: {e}")
            if attempt == max_retries:
                raise  # kalau sudah max retry, lempar error
            time.sleep(2)  # tunggu 2 detik sebelum nyoba lagi

# @timed
def df__sum_kategori(input_param,year=None):
    if year is None: year = datetime.now().year 
    else: year = int(year)
    store_list =input_param['store_list']
    if store_list  is not None:
        if store_list:
            placeholders = ", ".join([f"'{s}'" for s in store_list])
            df_cus = df__customers_pb_n(placeholders)
            whconsi_list = df_cus['fn_whconsiid'].tolist()
            
            if whconsi_list:
                whconsi_placeholders = ", ".join([str(s) for s in whconsi_list])
                whconsi = f"AND fn_whconsiid IN ({whconsi_placeholders})"
            else: whconsi = ""  # atau kondisi default kalau kosong
    else: whconsi = ""  # jika input_param['store_list'] None
    if input_param['divisi'] is not None:
        divisi = input_param['divisi']
        divisi = f'and fv_divname="{divisi}"'
    else: divisi = ''
    bulan=input_param['bulan']
    main_=generate_main_query(bulan)
    sales_thru=f'sales_thru_pb_{str(year)[-2:]}'
    main_query = f"""
    select
    sales_thru.fv_catname kategory,
    sales_thru.stock_awal,
    sales_thru.total_stock,
    sales_thru.sales,
    ROUND(100.0 * sales_thru.sales / NULLIF(total_data.sales, 0), 2) AS sales_kontribusi,
        sales_thru.stock_akhir,
    ROUND(100.0 * sales_thru.stock_akhir / NULLIF(total_data.stock_akhir, 0), 2) AS stock_kontribusi
    from (

        select fv_catname , 
        CAST(stock_awal AS SIGNED) AS stock_awal, 
        CAST(stock_akhir + sales AS SIGNED) AS total_stock,
        CAST(sales AS SIGNED) AS sales, 
        CAST(stock_akhir AS SIGNED) AS stock_akhir from (

        select fv_catname, {main_}
            FROM gis_local.{sales_thru} a
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
            WHERE 1=1 {whconsi} {divisi}
            group by fv_catname
        union all

        select "TOTAL" as fv_catname, {main_}
            FROM gis_local.{sales_thru} a
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
            WHERE 1=1 {whconsi} {divisi} )sub
            ) sales_thru,
            (
                select fv_catname, {main_}
                FROM gis_local.{sales_thru} a
                left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
                WHERE 1=1 {whconsi} {divisi} 
            ) total_data
    """
    results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__trx_in_out(input_param, 
                         col='stock_backdate.fn_whconsiid, stock_backdate.fv_artsizecode, fv_barcode, fv_sizename, a.fv_configname,fv_divname, fv_stat,',
                         gb='stock_backdate.fn_whconsiid, stock_backdate.fv_artsizecode,fv_divname, fv_stat',
                         year=None,
                         cus='',
                         art='',
                         times='this_y',
                         having='', queri=False):
    where=add_where(input_param,cus=cus, art=art)
    whconsi_list=where['whconsi']
    divisi=where['divisi']
    cat=where['cat']
    artikel=where['artikel']
    ornal=where['ornal']
    sub_cat=where['sub_cat']
    if year is None: year = datetime.now().year 
    if times == 'this_y': 
        input_value = input_param['start']
    else: 
        input_value= input_param['start_last']
    sales_thru=f'sales_thru_pb_{str(year)[-2:]}'
    trxstx = f"trxstx_pb{str(year)[-2:]}"
    stock = f"fn_stock_{input_value[5:7]}"
    main_query = f"""
        SELECT {col}
            SUM(stock) AS stocks
        FROM (
           
            SELECT fn_whconsiid, fn_stock AS stock, b.fv_artsizecode ,fv_stat
            FROM gis_local.{trxstx} b
            LEFT JOIN artikel_pb a ON b.fv_artsizecode = a.fv_artsizecode
            WHERE fd_tgltrx BETWEEN '{str(year)[-2:]}-{input_value[5:7]}-01' AND '{str(year)[-2:]}-{input_value[5:10]}'
            {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat}
            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, b.fv_artsizecode , fv_stat
            FROM gis_local.trxrt_pb b
            LEFT JOIN artikel_pb a ON b.fv_artsizecode = a.fv_artsizecode
            WHERE fd_tgltrx < '{str(year)[-2:]}-{input_value[5:10]}' {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat}
        ) stock_backdate
        LEFT JOIN artikel_pb a ON a.fv_artsizecode = stock_backdate.fv_artsizecode
        LEFT JOIN customers_pb b ON stock_backdate.fn_whconsiid = b.fn_whconsiid
        WHERE 1=1 
        GROUP BY {gb}
        {having}
            """
    
    if queri==True: print(main_query)
    # Retry up to 5 times if error
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
            df = pd.DataFrame(results)
            return df  # success, langsung return
        except Exception as e:
            print(f"[Attempt {attempt}] Query gagal: {e}")
            if attempt == max_retries:
                raise  # kalau sudah max retry, lempar error
            time.sleep(2)  # tunggu 2 detik sebelum nyoba lagi

# @timed
def df__stock_now_filter(input_param, 
                         col='stock_backdate.fn_whconsiid, stock_backdate.fv_artsizecode, fv_barcode, fv_sizename, a.fv_configname,',
                         gb='stock_backdate.fn_whconsiid, stock_backdate.fv_artsizecode',
                         year=None,
                         cus='',
                         art='',
                         times='this_y',
                         having='', queri=False):
    where=add_where(input_param,cus=cus, art=art)
    whconsi_list=where['whconsi']
    divisi=where['divisi']
    cat=where['cat']
    artikel=where['artikel']
    ornal=where['ornal']
    sub_cat=where['sub_cat']
    # artikelsize=where['artikelsize']
    list_artsize=where['list_artsize']
    if year is None: year = datetime.now().year 
    if times == 'this_y': 
        input_value = input_param['start']
        month_sales=input_param['month_sales_ty']
        if month_sales: month_sales= f'and fn_month in ({month_sales})'
        else:month_sales=''
        year_sales=input_param['year_sales_ty']
        if year_sales: year_sales= f'and fn_year in ({year_sales})'
        else:year_sales=''
    else: 
        input_value= input_param['start_last']
        month_sales=input_param['month_sales_ly']
        if month_sales: month_sales= f'and fn_month in ({month_sales})'
        else:month_sales=''
        year_sales=input_param['year_sales_ly']
        if year_sales: year_sales= f'and fn_year in ({year_sales})'
        else:year_sales=''
    sales_thru=f'sales_thru_pb_{input_value[2:4]}'
    trxstx = f"trxstx_pb{input_value[2:4]}"
    stock = f"fn_stock_{input_value[5:7]}"
    main_query = f"""
        SELECT {col}
            SUM(stock) AS stocks
        FROM (
            SELECT fn_whconsiid, {stock} AS stock, b.fv_artsizecode 
            FROM gis_local.{sales_thru} b
            LEFT JOIN artikel_pb a ON b.fv_artsizecode = a.fv_artsizecode
            where 1=1 {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat} {list_artsize} 
            
            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, b.fv_artsizecode 
            FROM gis_local.{trxstx} b
            LEFT JOIN artikel_pb a ON b.fv_artsizecode = a.fv_artsizecode
            WHERE fd_tgltrx BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND '{input_value[0:4]}-{input_value[5:10]}'
            {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat} {list_artsize}  
            
            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, b.fv_artsizecode 
            FROM gis_local.trxrt_pb b
            LEFT JOIN artikel_pb a ON b.fv_artsizecode = a.fv_artsizecode
            WHERE fd_tgltrx <= '{input_value}' and (fv_stat = 'adj_out' or fv_stat = 'adj_in')
            {whconsi_list} {list_artsize} {cat} {artikel} {ornal} {divisi} {sub_cat} 

            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, b.fv_artsizecode 
            FROM gis_local.trxrt_pb b
            LEFT JOIN artikel_pb a ON b.fv_artsizecode = a.fv_artsizecode
            WHERE fd_tgltrx BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND '{input_value}'
            {whconsi_list} {list_artsize} {cat} {artikel} {ornal} {divisi} {sub_cat} 

            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, b.fv_artsizecode 
            FROM gis_local.Stock_varian_{input_value[2:4]} b
            LEFT JOIN artikel_pb a ON b.fv_artsizecode = a.fv_artsizecode
            WHERE fd_tgl BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND '{input_value[0:4]}-{input_value[5:10]}'
            {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat} {list_artsize} 
        ) stock_backdate
        LEFT JOIN artikel_pb a ON a.fv_artsizecode = stock_backdate.fv_artsizecode
        LEFT JOIN customers_pb b ON stock_backdate.fn_whconsiid = b.fn_whconsiid
        WHERE 1=1 
        GROUP BY {gb}
        {having}
        """
    if queri==True: print(main_query)
    # Retry up to 5 times if error
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
            df = pd.DataFrame(results)
            return df  # success, langsung return
        except Exception as e:
            print(f"[Attempt {attempt}] Query gagal: {e}")
            if attempt == max_retries:
                raise  # kalau sudah max retry, lempar error
            time.sleep(2)  # tunggu 2 detik sebelum nyoba lagi

def df__stock_now_all_store(input_param, 
                         col='stock_backdate.fn_whconsiid, stock_backdate.fv_artsizecode, fv_barcode, fv_sizename, a.fv_configname,',
                         gb='stock_backdate.fn_whconsiid, stock_backdate.fv_artsizecode',
                         year=None,
                         cus='',
                         art='',
                         having='', queri=False):
    
    where=add_where(input_param,cus=cus, art=art)
    whconsi_list=where['whconsi']
    divisi=where['divisi']
    cat=where['cat']
    artikel=where['artikel']
    ornal=where['ornal']
    sub_cat=where['sub_cat']
    if year is None: year = datetime.now().year 
    today=datetime.now().strftime('%Y-%m-%d')
    sales_thru=f'sales_thru_pb_{today[2:4]}'

    main_query = f"""
        SELECT {col}
            SUM(stock) AS stocks
        FROM (
            SELECT fn_whconsiid, fn_stock_at AS stock, b.fv_artsizecode 
            FROM gis_local.{sales_thru} b
            LEFT JOIN artikel_pb a ON b.fv_artsizecode = a.fv_artsizecode
            where 1=1 {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat}
            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, b.fv_artsizecode 
            FROM gis_local.trxrt_pb b
            LEFT JOIN artikel_pb a ON b.fv_artsizecode = a.fv_artsizecode
            WHERE fd_tgltrx <= '{today}' {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat}
        ) stock_backdate
        LEFT JOIN artikel_pb a ON a.fv_artsizecode = stock_backdate.fv_artsizecode
        LEFT JOIN customers_pb b ON stock_backdate.fn_whconsiid = b.fn_whconsiid
        WHERE 1=1 
        GROUP BY {gb}
        {having}
        """
    if queri==True: print(main_query)
    # Retry up to 5 times if error
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
            df = pd.DataFrame(results)
            return df  # success, langsung return
        except Exception as e:
            print(f"[Attempt {attempt}] Query gagal: {e}")
            if attempt == max_retries:
                raise  # kalau sudah max retry, lempar error
            time.sleep(2)  # tunggu 2 detik sebelum nyoba lagi
# @timed
def df__stock_early_filter(input_param, 
                         col='stock_backdate.fn_whconsiid, fv_artsizecode, fv_barcode, fv_sizename, fv_configname,',
                         gb='stock_backdate.fn_whconsiid, fv_artsizecode',
                         year=None,
                         cus='',
                         art='',
                         times='this_y', queri=False):
    where=add_where(input_param,cus=cus, art=art)
    if year is None: year = datetime.now().year 
    if times == 'this_y': 
        input_value = input_param['end']
    else: 
        input_value= input_param['end_last']
    sales_thru=f'sales_thru_pb_{input_value[2:4]}'
    trxstx = f"trxstx_pb{input_value[2:4]}"
    stock = f"fn_stock_{input_value[5:7]}"
    whconsi_list=where['whconsi']
    divisi=where['divisi']
    cat=where['cat']
    artikel=where['artikel']
    ornal=where['ornal']
    main_query = f"""
        SELECT {col}
            SUM(stock) AS stocks_early
        FROM (
            SELECT fn_whconsiid, {stock} AS stock, fv_artsizecode 
            FROM gis_local.{sales_thru} 
            WHERE 1=1 {whconsi_list}
            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
            FROM gis_local.{trxstx} 
            WHERE fd_tgltrx BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND 
            '{input_value[0:4]}-{input_value[5:10]}' {whconsi_list}

            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, b.fv_artsizecode 
            FROM gis_local.trxrt_pb b
            LEFT JOIN artikel_pb a ON b.fv_artsizecode = a.fv_artsizecode
            WHERE fd_tgltrx <= '{input_value}' and (fv_stat = 'adj_out' or fv_stat = 'adj_in')
            {whconsi_list} {cat} {artikel} {ornal} {divisi}

            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, b.fv_artsizecode 
            FROM gis_local.trxrt_pb b
            LEFT JOIN artikel_pb a ON b.fv_artsizecode = a.fv_artsizecode
            WHERE fd_tgltrx BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND '{input_value}'
            {whconsi_list} {cat} {artikel} {ornal} {divisi} 

            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
            FROM gis_local.Stock_varian_{input_value[2:4]}
            WHERE fd_tgl BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND 
            '{input_value[0:4]}-{input_value[5:10]}' {whconsi_list}
        ) stock_backdate
        LEFT JOIN artikel_pb a ON a.fv_artsizecode = stock_backdate.fv_artsizecode
        LEFT JOIN customers_pb b ON stock_backdate.fn_whconsiid = b.fn_whconsiid
        WHERE 1=1 {cat} {artikel} {ornal} {divisi}
        GROUP BY {gb}
        """
    if queri==True: print(main_query)
    # Retry up to 5 times if error
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
            df = pd.DataFrame(results)
            return df  # success, langsung return
        except Exception as e:
            print(f"[Attempt {attempt}] Query gagal: {e}")
            if attempt == max_retries:
                raise  # kalau sudah max retry, lempar error
            time.sleep(2)  # tunggu 2 detik sebelum nyoba lagi
    # results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
    # df = pd.DataFrame(results)
    # return df

# @timed
def df__count_stock_now_filter(input_param, 
                         col='stock_backdate.fn_whconsiid, fv_artsizecode, fv_barcode, fv_sizename, fv_configname,',
                         gb='stock_backdate.fn_whconsiid, fv_artsizecode',
                         year=None,
                         cus='',
                         art='',
                         col_count='fv_barcode,',
                         times='this_y',
                         first_last='last'):
    where=add_where(input_param,cus=cus, art=art)
    if year is None: year = datetime.now().year 
    if times == 'this_y':
        if first_last == 'last':
            input_value = input_param['input_date']
        else:
            input_value = input_param['end']
    else:
        input_value = input_param['start_last']
    sales_thru=f'sales_thru_pb_{input_value[2:4]}'
    trxstx = f"trxstx_pb{input_value[2:4]}"
    stock = f"fn_stock_{input_value[5:7]}"
    whconsi_list=where['whconsi']
    divisi=where['divisi']
    cat=where['cat']
    artikel=where['artikel']
    ornal=where['ornal']
    main_query = f"""
        SELECT {col} COUNT(*) AS art_count
        FROM(   
            SELECT {col} {col_count}
                sum(stock) AS stocks
            FROM (
                SELECT fn_whconsiid, {stock} AS stock, fv_artsizecode 
                FROM gis_local.{sales_thru} WHERE 1=1 {whconsi_list} 
                UNION ALL
                SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
                FROM gis_local.{trxstx} 
                WHERE fd_tgltrx BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND 
                '{input_value[0:4]}-{input_value[5:10]}' {whconsi_list}

                UNION ALL
                SELECT fn_whconsiid, fn_stock AS stock, b.fv_artsizecode 
                FROM gis_local.trxrt_pb b
                LEFT JOIN artikel_pb a ON b.fv_artsizecode = a.fv_artsizecode
                WHERE fd_tgltrx <= '{input_value}' and (fv_stat = 'adj_out' or fv_stat = 'adj_in')
                {whconsi_list} 

                UNION ALL
                SELECT fn_whconsiid, fn_stock AS stock, b.fv_artsizecode 
                FROM gis_local.trxrt_pb b
                LEFT JOIN artikel_pb a ON b.fv_artsizecode = a.fv_artsizecode
                WHERE fd_tgltrx BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND '{input_value}'
                 {whconsi_list} 

                UNION ALL
                SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
                FROM gis_local.Stock_varian_{input_value[2:4]}
                WHERE fd_tgl BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND 
                '{input_value[0:4]}-{input_value[5:10]}' {whconsi_list}
            ) stock_backdate
            LEFT JOIN artikel_pb a ON a.fv_artsizecode = stock_backdate.fv_artsizecode
            LEFT JOIN customers_pb b ON stock_backdate.fn_whconsiid = b.fn_whconsiid
            WHERE 1=1 {cat} {artikel} {ornal} {divisi}
            GROUP BY {col_count} {gb}
            HAVING stocks>0) AS filtered_barcode
        GROUP BY filtered_barcode.{gb}
            """
    # # printol(main_query)
    # # printol(year)
    # Retry up to 5 times if error
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
            df = pd.DataFrame(results)
            return df  # success, langsung return
        except Exception as e:
            print(f"[Attempt {attempt}] Query gagal: {e}")
            if attempt == max_retries:
                raise  # kalau sudah max retry, lempar error
            time.sleep(2)  # tunggu 2 detik sebelum nyoba lagi
    # results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
    # df = pd.DataFrame(results)
    # return df

# @timed
def df__sum_kategori(input_param,year=None):
    if year is None: year = datetime.now().year 
    else: year = int(year)
    store_list =input_param['store_list']
    if store_list  is not None:
        if store_list:
            placeholders = ", ".join([f"'{s}'" for s in store_list])
            df_cus = df__customers_pb_n(placeholders)
            whconsi_list = df_cus['fn_whconsiid'].tolist()
            
            if whconsi_list:
                whconsi_placeholders = ", ".join([str(s) for s in whconsi_list])
                whconsi = f"AND fn_whconsiid IN ({whconsi_placeholders})"
            else: whconsi = ""  # atau kondisi default kalau kosong
    else: whconsi = ""  # jika input_param['store_list'] None
    if input_param['divisi'] is not None:
        divisi = input_param['divisi']
        divisi = f'and fv_divname="{divisi}"'
    else: divisi = ''
    bulan=input_param['bulan']
    main_=generate_main_query(bulan)
    sales_thru=f'sales_thru_pb_{str(year)[-2:]}'
    main_query = f"""
    select
    sales_thru.fv_catname kategory,
    sales_thru.stock_awal,
    sales_thru.total_stock,
    sales_thru.sales,
    ROUND(100.0 * sales_thru.sales / NULLIF(total_data.sales, 0), 2) AS sales_kontribusi,
        sales_thru.stock_akhir,
    ROUND(100.0 * sales_thru.stock_akhir / NULLIF(total_data.stock_akhir, 0), 2) AS stock_kontribusi
    from (

        select fv_catname , 
        CAST(stock_awal AS SIGNED) AS stock_awal, 
        CAST(stock_akhir + sales AS SIGNED) AS total_stock,
        CAST(sales AS SIGNED) AS sales, 
        CAST(stock_akhir AS SIGNED) AS stock_akhir from (

        select fv_catname, {main_}
            FROM gis_local.{sales_thru} a
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
            WHERE 1=1 {whconsi} {divisi}
            group by fv_catname
        union all

        select "TOTAL" as fv_catname, {main_}
            FROM gis_local.{sales_thru} a
            left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
            WHERE 1=1 {whconsi} {divisi} )sub
            ) sales_thru,
            (
                select fv_catname, {main_}
                FROM gis_local.{sales_thru} a
                left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
                WHERE 1=1 {whconsi} {divisi} 
            ) total_data
    """
    results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

# @timed
def df__sales_filter(input_param, 
                         col='stock_backdate.fn_whconsiid, fv_artsizecode, fv_barcode, fv_sizename,',
                         gb='fv_barcode, fn_whconsiid',
                         year=None,
                         cus='stock_backdate.',
                         art='',
                         times='this_y',
                         having='',
                         queri=False,spesial_case=''):
    where=add_where(input_param,cus=cus, art=art)
    if year is None: year = datetime.now().year 
    if times == 'this_y':
        start_str = input_param['start']
        ends_str = input_param['end']
        input_value = input_param['input_date']
        month_sales=input_param['month_sales_ty']
        if month_sales: month_sales= f'and fn_month in ({month_sales})'
        else:month_sales=''
        year_sales=input_param['year_sales_ty']
        if year_sales: year_sales= f'and fn_year in ({year_sales})'
        else:year_sales=''
    else: 
        start_str = input_param['start_last']
        ends_str = input_param['end_last']
        input_value= input_param['start_last']
        month_sales=input_param['month_sales_ly']
        if month_sales: month_sales= f'and fn_month in ({month_sales})'
        else:month_sales=''
        year_sales=input_param['year_sales_ly']
        if year_sales: year_sales= f'and fn_year in ({year_sales})'
        else:year_sales=''    
       
    if year==None: 
        sales = f"pb_penjualan_20{input_value[2:4]}"
        sales_last_year=f'pb_penjualan_20{int(input_value[2:4])-1}'
    else: 
        sales = f"pb_penjualan_20{input_value[2:4]}"
        sales_last_year=f'pb_penjualan_20{int(input_value[2:4])-1}'
    trxstx = f"trxstx_pb{input_value[2:4]}"
    stock = f"fn_stock_{input_value[5:7]}"
    whconsi_list=where['whconsi']
    divisi=where['divisi']
    cat=where['cat']
    artikel=where['artikel']
    if input_param['ornal'] is not None and input_param['ornal']!='':
        ornal_ori = input_param['ornal']
        ornal = f'and stock_backdate.fv_ornal="{ornal_ori}"'
        if spesial_case=="repeat_order":
            ornal = f'and a.fv_ornal="{ornal_ori}"'
    else: ornal = ''
    if col=="fv_ornal,":
        add="stock_backdate."
    else:add=""
    sub_cat=where['sub_cat']
    query = f"""select
        {col} sum(pcs) pcs, sum(sales) sales from (
        SELECT {add}{col} sum(fn_jualpersize) as pcs, sum(fn_totalpenjualan) sales FROM gis_local.{sales} stock_backdate 
        LEFT JOIN artikel_pb a ON a.fv_artsizecode = stock_backdate.fv_artsizecode
        LEFT JOIN customers_pb b ON stock_backdate.fn_whconsiid = b.fn_whconsiid
        where fd_tgltrx between '{ends_str}' and '{start_str}' {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat} {year_sales} {month_sales}
        group by {add}{gb}
        union all
        SELECT {add}{col} sum(fn_jualpersize) as pcs, sum(fn_totalpenjualan) sales FROM gis_local.{sales_last_year} stock_backdate 
        LEFT JOIN artikel_pb a ON a.fv_artsizecode = stock_backdate.fv_artsizecode
        LEFT JOIN customers_pb b ON stock_backdate.fn_whconsiid = b.fn_whconsiid
        where fd_tgltrx between '{ends_str}' and '{start_str}' {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat} {year_sales} {month_sales}
        group by {add}{gb}
        union all
        SELECT {add}{col} sum(fn_jualpersize) as pcs, sum(fn_totalpenjualan) sales FROM gis_local.pb_penjualan_today stock_backdate 
        LEFT JOIN artikel_pb a ON a.fv_artsizecode = stock_backdate.fv_artsizecode
        LEFT JOIN customers_pb b ON stock_backdate.fn_whconsiid = b.fn_whconsiid
        where fd_tgltrx between '{ends_str}' and '{start_str}' {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat} {year_sales} {month_sales}
        group by {add}{gb})stock_backdate
        group by {gb}
        {having}
        """
    
    if queri==True: print(query)
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    # df = pd.read_sql(query, engine_imp)
    return df

# @timed
def df__detail_trx(input_param, 
                    col='fn_whconsiid, fv_catname, fv_stat,',
                    gb='fn_whconsiid, fv_catname, fv_stat',
                    year=None,
                    cus='',
                    art='',check_stock=False):
    where=add_where(input_param,cus=cus, art=art)
    if year is None: year = datetime.now().year 
    start_str = str(year) + (input_param['start'])[4:]
    ends_str = str(year) + (input_param['end'])[4:]
    if check_stock==True:
        start_str = (input_param['start'])
        ends_str = (input_param['end'])
        
    input_value = input_param['input_date']
    whconsi_list=where['whconsi']
    divisi=where['divisi']
    cat=where['cat']
    artikel=where['artikel']
    ornal=where['ornal']
    
    query = f''' SELECT {col} stock FROM (
    SELECT {col} abs(sum(fn_stock)) stock FROM gis_local.trxstx_pb25 a 
    left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
    where 1=1 and fd_tgltrx between '{ends_str}' and '{start_str}' {whconsi_list} {cat} {divisi} {ornal}
    group by {gb} 
    union all
    SELECT {col} abs(sum(fn_stock)) stock FROM gis_local.trxstx_pb24 a 
    left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
    where 1=1 and fd_tgltrx between '{ends_str}' and '{start_str}' {whconsi_list} {cat} {divisi} {ornal}
    group by {gb} 
    union all
    SELECT {col} abs(sum(fn_stock)) stock FROM gis_local.trxstx_pb23 a 
    left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
    where 1=1 and fd_tgltrx between '{ends_str}' and '{start_str}' {whconsi_list} {cat} {divisi} {ornal}
    group by {gb} 
    union all
    SELECT {col} abs(sum(fn_stock)) stock FROM gis_local.trxstx_pb22 a 
    left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
    where 1=1 and fd_tgltrx between '{ends_str}' and '{start_str}' {whconsi_list} {cat} {divisi} {ornal}
    group by {gb} 
    union all
    SELECT {col} abs(sum(fn_stock)) stock FROM gis_local.trxrt_pb a 
    left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
    where 1=1 and fd_tgltrx between '{ends_str}' and '{start_str}' {whconsi_list} {cat} {divisi} {ornal}
    group by {gb}) good
        '''
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    return df

# @timed
def df__stok_awal_akhir(input_param, 
                    col='fn_whconsiid, fv_catname,',
                    gb='fn_whconsiid, fv_catname',
                    year=None,
                    cus='',
                    art='',):
    # contoh input
    selected_month = int(input_param['bulan'])   # Agustus
    current_month = datetime.today().month  # bulan berjalan
    where=add_where(input_param,cus=cus, art=art)
    whconsi_list=where['whconsi']
    divisi=where['divisi']
    cat=where['cat']
    artikel=where['artikel']
    ornal=where['ornal']
    if selected_month == current_month:
        # jika bulan dipilih = bulan berjalan
        stock_sum_query = f"sum(fn_stock_{str(selected_month).zfill(2)}), sum(fn_stock_at)"
    else:
        # jika bulan lebih kecil dari bulan berjalan, ambil sampai bulan itu
        stock_cols = [f"sum(fn_stock_{str(i).zfill(2)})" for i in range(selected_month , selected_month + 2)]
        stock_sum_query = ", ".join(stock_cols)

    query = f"""
    SELECT {col} {stock_sum_query}
    FROM gis_local.sales_thru_pb_25 a
    left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
    where 1=1 {whconsi_list} {cat}
    GROUP BY {gb};
    """
    # # printol(query)
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    return df

# @timed
def df__alok_from_subid(input_param,whconsi=False):
    pic_mic_dict = {
        3079: 'IRENE/NISA',
        # 26: 'TYAS',
        # 3348: 'TYAS / YUNI',
        # 200: 'VINDRA',
        1585: 'DIAN/EKA',
        3107: 'NOPAL',
        147: 'SHOFI',
        3350: 'IRENE/NISA',
        # 1804: 'MILI',
        # 3341: 'MILI/NENI',
        1759: 'NOVA/ANGGI',
        40: 'SYAFII',
        3999:['DIAN/EKA','NOVA/ANGGI'],
        3214:'IRENE/NISA'
    }
    subid = input_param.get('subid')
    q_alok=''
    # Normalize jadi list
    if subid is None:
        print(subid)
    else:
        # if not isinstance(subid, list):
        #     subid = [subid]
        
        # # Filter hanya subid yang ada di dalam dict
        # alokator_list = [pic_mic_dict[int(s)] for s in subid if int(s) in pic_mic_dict]
        alokator_list = []
        for s in subid:
            val = pic_mic_dict.get(int(s))
            if val:
                if isinstance(val, list):
                    alokator_list.extend(val) # Tambahkan isi listnya
                else:
                    alokator_list.append(val) # Tambahkan stringnya

        if alokator_list:
            # Set digunakan untuk menghindari duplikat jika ada
            alokator_list_str = ", ".join(f"'{alok}'" for alok in set(alokator_list))
            q_alok = f"AND fv_alokator IN ({alokator_list_str})"
            
        if whconsi and q_alok:
            df_cus=df__customers_pb_('fn_whconsiid',where=q_alok)
            list_cus=list_to_sql_in(df_cus,'fn_whconsiid')
            q_alok=list_cus
        else:
            q_alok = ''  # kosong karena tidak ada yang cocok
    
    return q_alok
    # if subid is None:
    #     q_alok = ''
    # else:
    #     if not isinstance(subid, list):
    #         subid = [subid]
        
    #     # Filter hanya subid yang ada di dalam dict
    #     alokator_list = []
    #     for s in subid:
    #         val = pic_mic_dict.get(int(s))
    #         if val:
    #             if isinstance(val, list):
    #                 alokator_list.extend(val) # Tambahkan isi listnya
    #             else:
    #                 alokator_list.append(val) # Tambahkan stringnya

    #     if alokator_list:
    #         # Set digunakan untuk menghindari duplikat jika ada
    #         alokator_list_str = ", ".join(f"'{alok}'" for alok in set(alokator_list))
    #         q_alok = f"AND fv_alokator IN ({alokator_list_str})"

    #     if whconsi and q_alok:
    #         df_cus=df__customers_pb_('fn_whconsiid',where=q_alok)
    #         list_cus=list_to_sql_in(df_cus,'fn_whconsiid')
    #         q_alok=list_cus

    #     else:
    #         q_alok = ''  # kosong karena tidak ada yang cocok
    
    # return q_alok

# @timed
def df__spv_from_alok(input_param):
    q_alok=df__alok_from_subid(input_param)
    df=df__customers_pb_('fv_spv',distinct=True,where=q_alok)
    return df

# @timed
def df__for_obdtl(list_artikel):
    # printol("Input:", list_artikel)
    if list_artikel:
        # pastikan semua barcode bersih
        list_artikel = [str(x).strip().replace("’", "").replace("‘", "") for x in list_artikel]
        
        # gabungkan jadi string SQL, TANPA tanda kutip ganda tambahan
        list_art = ",".join([f"'{x}'" for x in list_artikel])
        artikel = f"and fv_barcode in ({list_art})"
        # printol("Filter SQL:", artikel)
    else: artikel=''
    query = f"""
    SELECT a.fv_artsizecode,  fv_articlecode, fv_articlename, a.fn_sizeid, fv_sizename size,
    fm_price fn_hargasatuan,fn_artsizeid, a.fn_articleid, fv_barcode, fn_divid, 
    fn_brandid, d.fm_amount fn_hargasatuan 
    FROM gis_db.articlesize_tb a
    left join gis_db.article_tb b on a.fn_articleid=b.fn_articleid 
    left join gis_db.size_tb c on a.fn_sizeid=c.fn_sizeid
    left join gis_db.markdown_tb d on b.fn_markid=d.fn_markid 
    where 1=1 {artikel};
    """
    # printol('dtl')
    # printol(query)
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbsave))
    return df

# @timed
def df__for_obmst(list_cusid):
    if list_cusid:
        list_cusid = ", ".join([str(s) for s in list_cusid])
        cusid = f"and fn_cusid IN ({list_cusid})"
    else: cusid=''
    query = f"""
    SELECT fn_cusid,a.fn_grpcusid, fv_namecus fv_namacust, a.fv_address, 
    a.fv_phonenumber,a.fv_address fv_alamatkirim, fn_discid FROM gis_db.customer_tb a
    left join gis_db.groupcustomer_tb b on a.fn_grpcusid=b.fn_grpcusid
    where 1=1 {cusid};
    """
    # printol('mst')
    # printol(query)
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbsave))
    return df

# @timed
def df__m_sum_draft_ob():
    query = f"""
    SELECT fn_whconsiid, fv_ornal, fv_divname, sum(fn_on_book) FROM gis_local.t_draftdtl a 
    left join gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
    where fc_status= "X"
    group by fn_whconsiid,fv_ornal, fv_divname;
    """
    # printol('mst')
    # printol(query)
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    return df

# @timed
def df__sum_draft_ob():
    query = f"""
    SELECT concat(fc_code," - ", fv_toko)  fc_code, fv_ornal, fv_divname, 
    a.fv_artsizecode, a.fv_barcode Artikel, fv_sizename Size, sum(fn_on_book) qty FROM gis_local.t_draftdtl a 
    left join gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
    left join gis_local.customers_pb c on a.fn_whconsiid=c.fn_whconsiid
    where fc_status= "X"
    group by fc_code,fv_ornal, fv_divname, a.fv_artsizecode;
    """
    # printol('mst')
    # printol(query)
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    return df

# @timed
def df__pre_rt(type_data='mst', detail_id=None):
    if type_data=='mst':
        data='t_returmst_pre'
    else : data='t_returdtl_pre'
    query = f"""SELECT * FROM {data} WHERE fv_noretur = '{detail_id}'
    """
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbsave))
    return df

# @timed
def df__pre_mt(type_data='mst', detail_id=None):
    if type_data=='mst':
        data='t_mutasimst_pre'
    else : data='t_mutasidtl_pre'
    query = f"""SELECT * FROM {data} WHERE fv_nomutasi = '{detail_id}'
    """
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbsave))
    return df

# @timed
def df__pre_ob(type_data='mst', detail_id=None):
    if type_data=='mst':
        data='t_ordermst_pos_pre'
        query = f"""
        SELECT fv_noorder, 
        "-" as fv_noresikurir,
        fn_cusid,
        concat('Gudang ', fv_whtypename) fn_whid,
        fv_keteranganumum, 
        fn_userid,
        fc_status,
        "" as fv_keterangantambahan,
        fn_userid_verif fn_useridspg,
        fv_noorder_real,
        fd_tglorder,
        "-" fv_dokumen,
        fd_tgldelivery

        FROM {data} a
        Left join gis_db.warehousetype_tb b on a.fn_whid=b.fn_whtypeid
        WHERE fv_noorder = '{detail_id}'
        """
    else : 
        data='t_orderdtl_goods_tmp_pos_pre'
        query = f"""SELECT 
        fn_idurut,
        fv_noorder,
        fv_artsizecode,
        fn_orderpersize,
        fc_status
        FROM {data} 
        WHERE fv_noorder = '{detail_id}'
        """
    
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbsave))
    return df

# @timed
def df__ostpo(where='',ost=True):
    if ost:
        gr='left join gis_db.grmst_tb c on a.fc_nodoc=c.fv_nopo'
        condition_gr='and c.fc_nodoc is null'
    else:gr,condition_gr='',''
    query = f'''
        SELECT distinct fv_barcode, fv_sizename, fn_qty from gis_db.podtl_tb a
        inner join gis_db.pomst_tb b on a.fc_nodoc=b.fc_nodoc
        {gr}
        left join gis_db.pimst_tb g on b.fc_nopi=g.fc_nodoc
        left join gis_db.article_tb e on a.fn_itemid=e.fn_articleid
        left join gis_db.size_tb f on a.fn_sizeid=f.fn_sizeid
        where  b.fn_potypeid in (131,134) and b.fn_pocatid in (141,135) {condition_gr} and fn_itemid=g.fn_articleid 
            {where}'''
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbsave))
    return df

# @timed
def df__pi(col="*", where='', distinct=False, queri=False):
    # if ost:
    #     gr='left join gis_db.grmst_tb c on a.fc_nodoc=c.fv_nopo'
    #     condition_gr='and c.fc_nodoc is null'
    # else:gr,condition_gr='',''
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}' 
    query = f'''
        SELECT {distinct} {col}  FROM pimst_tb
        where 1=1 {where}'''
    if queri==True: print(query)
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbsave))
    return df

# @timed
def df__grpsup(col="*", where='', distinct=False):
    # if ost:
    #     gr='left join gis_db.grmst_tb c on a.fc_nodoc=c.fv_nopo'
    #     condition_gr='and c.fc_nodoc is null'
    # else:gr,condition_gr='',''
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}' 
    query = f'''SELECT {distinct} {col} FROM groupsupplier_tb  
    where 1=1 {where}'''
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbsave))
    return df

# @timed
def df__spk(col="", where='', distinct=False,queri=False):
    # if ost:
    #     gr='left join gis_db.grmst_tb c on a.fc_nodoc=c.fv_nopo'
    #     condition_gr='and c.fc_nodoc is null'
    # else:gr,condition_gr='',''
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}' 
    query = f'''SELECT {distinct} 
    pi.fc_nodoc "No_PI",  
    a.fv_barcode Article, 
    pi.fn_grpsupid,
    b.fv_namegrpsup "Supplier_Name", 
    b.fc_codegrpsup "Supplier_Code", 
    pi.fv_grouppi "grup_pi",
    c.fv_configname "PI_Type", 
    ifnull(pi.fn_totalqty,0) "QTY_Plan", 
    ifnull(pi.fn_qtymarker,0) "QTY_Marker", 
    ifnull(pi.fn_qtycutting,0) "QTY_Cut", 
    ifnull(gr.fd_arrivaldate,"-") "Arrival_Date",
    ifnull(gr.fn_totalqty,0) "Qty_Masuk",
    gr.fd_arrivaldate fd_date
    -- pi.fv_notes "Notes" 
    {col} 
    FROM pimst_tb pi 
    left join article_tb a on  pi.fn_articleid=a.fn_articleid
    left join config_tb c on pi.fn_typeid=c.fn_configid
    left join groupsupplier_tb b on pi.fn_grpsupid=b.fn_grpsupid
    left join 
    (SELECT fv_desc, fn_totalqty, DATE(fd_arrivaldate) AS fd_arrivaldate 
        FROM gis_db.grmst_tb
        UNION ALL 
        SELECT fv_desc, fn_totalqty, DATE(fd_dateinput) AS fd_dateinput 
        FROM gis_db.stockinmst_tb) gr on pi.fc_nodoc=gr.fv_desc
    where 1=1 {where}
    order by gr.fd_arrivaldate'''
    if queri==True:print(query)
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbsave))
    # df=df.drop(columns=['fc_nodoc','fv_notes','fn_grpsupid','fn_totalqty'])
    return df

# @timed
def df__stok_po(set_minus=True):
    if set_minus is True: set_minus='* -1'
    else: set_minus=''
    query = f'''
        SELECT fv_barcode, fv_artsizecode, sum(fn_on_book {set_minus}) fn_qty 
        FROM gis_local.t_pre_order_dtl where fc_status="X"
        group by fv_artsizecode
          '''
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    return df
# @timed
def df__stock_gr(where=''):
    query = f'''
        SELECT fv_artsizecode, sum(fn_qty) fn_qty, fv_nopo FROM gis_db.grdtl_tb a 
        left join gis_db.grmst_tb c on a.fc_nodoc=c.fc_nodoc 
        where 1=1 {where}
        group by fv_artsizecode, fv_nopo 
            '''
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbsave))
    return df
def df__toko_putus():
    query = '''
        SELECT fn_cusid, concat(fc_codecus, ' - ', fv_namecus) fv_toko FROM gis_db.customer_tb a
        inner join gis_db.groupcustomer_tb b on a.fn_grpcusid=b.fn_grpcusid
        where fn_typecust=59 and a.fc_hold="F" 
            '''
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbsave))
    return df
# @timed
def df__tglsa(idwhtoko, tglnow):
    """
    Fungsi untuk mendapatkan tanggal Opname terakhir berdasarkan idwhtoko dan tglnow.
    """
    query = f'''
    SELECT a.fn_whconsiid, fd_tglinject AS fd_tglflag, DATE_FORMAT(fd_tglinject, '%%Y-%%m-%%d') AS fd_tglflagIndo, "X" as fc_status
    FROM saldoawalarticleconsi_tb a
    WHERE 1=1 {idwhtoko} AND SUBSTR(fd_tglinject, 1, 10) <= '{tglnow}'
    '''
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbsave))
    return df
# @timed
def df__tglso(idwhtoko, tglnow):
    """
    Fungsi untuk mendapatkan tanggal Opname terakhir berdasarkan idwhtoko dan tglnow.
    """
    query = f'''
            SELECT DISTINCT a.fn_whconsiid, a.fd_tglflag, DATE_FORMAT(a.fd_tglflag, '%%Y-%%m-%%d') AS fd_tglflagIndo, a.fc_status
            FROM gis_db.fl_stockarticleconsipostingmst a
            WHERE 1=1 {idwhtoko} AND SUBSTR(a.fd_tglflag, 1, 10) <= '{tglnow}' and fc_status="X"
            '''
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbsave))
    return df

# @timed
def df__salesthru(  input_param,
                    year, 
                    year_short='',
                    col='salesthru.fn_whconsiid,salesthru.fv_artsizecode,',
                    gb='salesthru.fn_whconsiid,salesthru.fv_artsizecode',
                    cus='b.',
                    art='', 
                    queri=False ):
    if year is None: year = datetime.now().year 
    year=str(year)[-2:]
    where=add_where(input_param,cus=cus, art=art)
    whconsi_list=where['whconsi']
    divisi=where['divisi']
    cat=where['cat']
    artikel=where['artikel']
    ornal=where['ornal']
    sub_cat=where['sub_cat']
    # artikelsize=where['artikelsize']
    list_artsize=where['list_artsize']

    input_month=input_param['month_sales_ly']
    month_list = [int(x) for x in input_month.split(',')]
    max_val = max(month_list)
    if not max_val: 
        max_val=datetime.now().month + 1
    if max_val>12:max_val='at'
    else:max_val = f"{max_val:02d}"
        
    sales_thru=f'sales_thru_pb_{year}'
    stock = f"fn_stock_{max_val}"
    query_sum = f"sum({'+ '.join([f'fn_qty_{m:02d}' for m in month_list])})"
    """
    Fungsi untuk mendapatkan tanggal Opname terakhir berdasarkan idwhtoko dan tglnow.
    """
    query = f'''
            SELECT {col} {stock} stock_{year_short}, 
            {query_sum} sales_{year_short}
            FROM {sales_thru} salesthru
            
            LEFT JOIN artikel_pb a ON a.fv_artsizecode = salesthru.fv_artsizecode
            LEFT JOIN customers_pb b ON salesthru.fn_whconsiid = b.fn_whconsiid
            WHERE 1=1 {whconsi_list} {cat} {artikel} {ornal} {divisi} {sub_cat} {list_artsize} 
            group by {gb}
            having sales_{year_short} > 0
            '''
    
    if queri==True:print(query)
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    return df


def df__penju(year):
    query = f'''SELECT 
        b.fn_idurut, 
        a.fv_nopenjualan, 
        a.fn_cusid fn_whconsiid,
        b.fv_artsizecode, 
        b.fn_jualpersize ,
        b.fn_hargasatuan ,
        b.fn_hargasatuan*b.fn_jualpersize as total_penjualan2,
        DATE_FORMAT(a.fd_tgljual, '%%m-%%d-%%Y') tgltrx,
        b.fn_discid disc_id, 
        b.fv_namemark status_artikel, b.fv_descevent "desc",
        b.fn_tabel,
        d.fv_namemark 'event'
    FROM 
        t_penjualanmst a 
    INNER JOIN 
        t_penjualandtl_goods b ON a.fv_nopenjualan = b.fv_nopenjualan
    LEFT JOIN 
        (SELECT a.fn_discid, b.fv_namemark FROM discount_tb a
            INNER JOIN markdown_tb b ON a.fn_markid = b.fn_markid) d ON b.fn_discid = d.fn_discid
    WHERE 
        a.fv_nopenjualan LIKE 'PJ%%' and b.fc_status <> 'F' 
        and a.fc_status <> 'F' 
        and a.fd_tgljual BETWEEN '{year}-01-01 00:00:01' and '{year}-12-31 23:59:59'
    '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df
