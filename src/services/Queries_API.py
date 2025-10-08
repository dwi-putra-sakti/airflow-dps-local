
# from services.Queries_API
import pandas as pd
import numpy as np
from core.db import DatabaseConnector

dbLoad = 'default'
dbsave = 'local'

def df__customers_pb_n(placeholders):
        query = f'''
            SELECT fn_cusid,fn_whconsiid FROM gis_local.customers_pb
            where fc_code in ({placeholders})
            '''
        # df = pd.read_sql(query, engine_imp)
        results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
        df = pd.DataFrame(results)
        return df

def df__customers_pb(placeholders,colom):
    query = f'''
        SELECT {colom} FROM gis_local.customers_pb
        where fc_code in ({placeholders})
        '''
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

def df__cus_list_get_all_distinct(colom):
    query = f'''
        SELECT distinct {colom} FROM gis_local.customers_pb
        '''
        # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

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

def df_artikel_pb_(colom):
    query = f'''
        SELECT {colom}
        FROM gis_local.artikel_pb
        '''
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

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

def df__main(input_param):
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

    main_query = f"""
    SELECT 
    stock_backdate.fn_whconsiid,
    SUM(stock) AS stocks,
    stock_backdate.fv_artsizecode, 
    fv_barcode, fv_sizename, fv_configname

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
    UNION ALL
    SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
    FROM gis_local.Stock_varian_25
    WHERE fd_tgl BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND '{input_value}'
    ) stock_backdate
    left join artikel_pb b on stock_backdate.fv_artsizecode = b.fv_artsizecode
    WHERE 1=1 and fn_whconsiid in ({whconsi_list})
    GROUP BY fn_whconsiid, fv_artsizecode
    """
    # df = pd.read_sql(main_query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

def df__main_stok_atas(input_param):
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
            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
            FROM gis_local.Stock_varian_25
            WHERE fd_tgl BETWEEN '{input_value[0:4]}-{input_value[5:7]}-01' AND '{input_value}'
        ) stock_backdate
        LEFT JOIN artikel_pb a ON a.fv_artsizecode = stock_backdate.fv_artsizecode
        LEFT JOIN customers_pb b ON stock_backdate.fn_whconsiid = b.fn_whconsiid
        WHERE 1=1 and stock_backdate.fn_whconsiid in ({whconsi_list})
        GROUP BY stock_backdate.fn_whconsiid, fv_artsizecode
        """
    results = DatabaseConnector.execute_raw_sql(main_query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

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
    SELECT * FROM gis_local.trxstx_pb25 
    where fv_stat like 'receive%%' and fd_tgltrx <= '{input_value}') a
    left join artikel_pb b on a.fv_artsizecode = b.fv_artsizecode
    WHERE 1=1 and fn_whconsiid in ({whconsi_list})
    group by fn_whconsiid,fv_barcode, fd_tgltrx 
    """
    # df = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
    df = pd.DataFrame(results)
    return df

def df__sales(input_param):
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
        SELECT fv_barcode, fn_jualpersize, fd_tgltrx, fv_sizename, fn_whconsiid, a.fv_artsizecode FROM gis_local.pb_penjualan_2025 a
        left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
        union all 
        SELECT fv_barcode, fn_jualpersize, fd_tgltrx, fv_sizename, fn_whconsiid, a.fv_artsizecode FROM gis_local.pb_penjualan_2024 a
        left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode) trx
        where trx.fd_tgltrx between '{ends_str}' and '{start_str}' and fn_whconsiid in ({whconsi_list})
        group by fv_barcode, fn_whconsiid       
        """
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    # df = pd.read_sql(query, engine_imp)
    return df

def df__retur(input_param):
    input_value = input_param['input_date']
    alokator = input_param['alokator']
    store_list = input_param['store_list']
    if store_list:
        placeholders = ", ".join([f"'{s}'" for s in store_list])
    df_cus = df__customers_pb_n(placeholders)
    whconsi_list=df_cus['fn_whconsiid'].tolist()
    whconsi_list =  ", ".join([f"'{s}'" for s in whconsi_list])

    query = f"""
    select a.fn_whconsiid, fv_barcode, sum(qty)*-1 as retur FROM gis_local.trx_retur a
    left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
    left join customers_pb c on a.fn_whconsiid=c.fn_whconsiid
    WHERE tgltrx <= '{input_value}' and a.fn_whconsiid in ({whconsi_list})
    group by fv_toko, fv_barcode
    """
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    # df = pd.read_sql(query, engine_imp)
    return df

def df__mutasi(input_param):
    input_value = input_param['input_date']
    alokator = input_param['alokator']
    store_list = input_param['store_list']
    if store_list:
        placeholders = ", ".join([f"'{s}'" for s in store_list])
    df_cus = df__customers_pb_n(placeholders)
    whconsi_list=df_cus['fn_whconsiid'].tolist()
    whconsi_list =  ", ".join([f"'{s}'" for s in whconsi_list])

    query = f"""
        SELECT a.fn_whconsiid, fv_barcode, sum(fn_qtykirim) Mutasi FROM 
        (SELECT fn_whconsiid, fv_artsizecode,fd_tgltransaksi, fn_qtykirim from gis_local.dt_mutasi_2025 
        where fv_userbatal is null and fd_tgltransaksi <= '{input_value}'
        union all  
        SELECT fn_whconsiid, fv_artsizecode,fd_tgltransaksi, fn_qtykirim from gis_local.dt_mutasi_2024
        where fv_userbatal is null and fd_tgltransaksi <= '{input_value}') a
        left join  gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        left join customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        where 1=1 and  a.fn_whconsiid in ({whconsi_list})
        group by fn_whconsiid, fv_barcode
        """
    
    df=pd.DataFrame(DatabaseConnector.execute_raw_sql(query, using=dbLoad))
    # df = pd.read_sql(query, engine_imp)
    return df

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

def change_cusid_to_whconsiid(df,df_cus):
                df=pd.merge(df,df_cus,how='left',on='fn_cusid')
                df['fn_cusid']=df['fn_whconsiid']
                df=df.drop(columns='fn_whconsiid')
                df=df.rename(columns={'fn_cusid':'fn_whconsiid'})
                return df

def df_ostdo_(input_param,col):
    # query = f'''
    #     SELECT fv_artsizecode,fv_barcode, fv_catname , fv_brandname ,
    #         fv_divname , fv_colorname, fv_rating,
    #         fv_configname, fv_namemark, fm_price from artikel_pb
    #     '''
    # results = execute_raw_sql(query)
    # df_art = pd.DataFrame(results)
    store_list = input_param['store_list']
    if store_list:
        placeholders = ", ".join([f"'{s}'" for s in store_list])
    df_cus = df__customers_pb_n(placeholders)
    cus_list= df_cus['fn_cusid'].tolist()
    cus_list =  ", ".join([f"'{s}'" for s in cus_list])

    query = f'''
            SELECT fv_artsizecode,fv_barcode from artikel_pb
            '''
    # df_art = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query)
    df_art = pd.DataFrame(results)
    query_1= f'''SELECT 
        c.fn_cusid ,fv_artsizecode, sum(fn_qty) OstDO
        FROM gis_db.dodtl_tb a 
        inner join gis_db.domst_tb b on a.fc_nodoc=b.fc_nodoc
        inner join gis_db.t_ordermst c on b.fc_noreff=fv_noorder
        left join gis_db.t_receivingmst d on b.fc_nodoc=d.fc_nosj
        where  fv_noreceiving is null and c.fn_cusid in ({cus_list}) and b.fc_status="F" 
        group by fn_cusid, fv_artsizecode, fd_date 
    '''
    results_1 = DatabaseConnector.execute_raw_sql(query_1, using=dbsave)
    df = pd.DataFrame(results_1)
    
    if not df.empty:
        df=change_cusid_to_whconsiid(df,df_cus)
        df=pd.merge(df, df_art, on='fv_artsizecode', how='left')
        if col == 'fv_artsizecode':
                return df
        df=df.groupby(['fn_whconsiid','fv_barcode'], as_index=False)['OstDO'].sum()
        df=pd.merge(df, df_art, on='fv_barcode', how='left').drop_duplicates('fv_barcode')
        df=df.drop(columns='fv_artsizecode')
        return df
    else:
        if col == 'fv_artsizecode':
                return pd.DataFrame(columns=['fn_whconsiid', 'fv_artsizecode', 'OstDO','fv_barcode'])
        return pd.DataFrame(columns=['fn_whconsiid', 'fv_barcode', 'OstDO'])
    
def df_ostdm_(input_param,col):
    store_list = input_param['store_list']
    if store_list:
        placeholders = ", ".join([f"'{s}'" for s in store_list])
    df_cus = df__customers_pb_n(placeholders)
    cus_list= df_cus['fn_cusid'].tolist()
    cus_list =  ", ".join([f"'{s}'" for s in cus_list])
    whconsi_list=df_cus['fn_whconsiid'].tolist()
    whconsi_list =  ", ".join([f"'{s}'" for s in whconsi_list])
    
    query = f'''
        SELECT fv_artsizecode,fv_barcode from artikel_pb
        '''
    # df_art = pd.read_sql(query, engine_imp)
    results = DatabaseConnector.execute_raw_sql(query)
    df_art = pd.DataFrame(results)
    query= f'''
        SELECT 
            fn_whconsiid2 fn_whconsiid,fv_artsizecode, sum(fn_qty) OstDM
            FROM gis_db.dodtl_tb a 
            inner join gis_db.domst_tb b on a.fc_nodoc=b.fc_nodoc
            inner join gis_db.t_mutasimst c on b.fc_noreff=c.fv_nomutasi
            left join gis_db.t_receivingmst d on b.fc_nodoc=d.fc_nosj
            where  fv_noreceiving is null and fn_whconsiid2 in ({whconsi_list})
            and b.fc_status="F" 
            group by fn_whconsiid2, fv_artsizecode, fd_date 
        '''
    # df = pd.read_sql(query, engine_write)
    results = DatabaseConnector.execute_raw_sql(query, using=dbsave)
    df = pd.DataFrame(results)
    if not df.empty:
        df=pd.merge(df, df_art, on='fv_artsizecode', how='left')
        if col == 'fv_artsizecode':
            return df
        df=df.groupby(['fn_whconsiid','fv_barcode',], as_index=False)['OstDM'].sum()
        return df
    else:
        if col == 'fv_artsizecode':
            return pd.DataFrame(columns=['fn_whconsiid', 'fv_artsizecode', 'OstDM','fv_barcode'])
        return pd.DataFrame(columns=['fn_whconsiid', 'fv_barcode', 'OstDM'])
    