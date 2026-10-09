from modules.db_connection import get_engine, get_db
from datetime import datetime, date
import pandas as pd
import numpy as np
def list_to_sql_in(df, col_name, astype='str'):
    """
    Mengubah kolom DataFrame menjadi string SQL untuk WHERE IN.
    Contoh hasil: "'A', 'B', 'C'"
    """
    if col_name not in df.columns:
        raise ValueError(f"Kolom '{col_name}' tidak ditemukan di DataFrame.")

    # Ambil kolom dan drop NaN
    values = df[col_name].dropna().astype(astype).tolist()

    # Jika kosong, return string kosong (hindari error di SQL)
    if not values:
        return "''"

    # Format jadi "'A', 'B', 'C'"
    sql_str = ", ".join([f"'{v}'" for v in values])
    return sql_str
def max_time_input(col_tgl_input,stat,table):
    db=get_db(stat)
    query = f'''
    select max({col_tgl_input}) max_input from {db}.{table}
    '''
    with get_engine(stat) as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df
def min_time_input(col_tgl_input,stat,table):
    db=get_db(stat)
    query = f'''
    select min({col_tgl_input}) min_input from {db}.{table}
    '''
    with get_engine(stat) as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df
def df__aging():
    input_value=datetime.today().strftime("%Y-%m-%d")
    query = f"""
    SELECT a.fn_whconsiid, fv_barcode, fn_stock, fd_tgltrx FROM 
    (SELECT * FROM gis_local.trxstx_pb24 
    where fv_stat like 'receive%%' and fd_tgltrx <= '{input_value}'
    union all
    SELECT fn_whconsiid,  
    fv_artsizecode,  
    fn_stock ,
    fd_tgltrx  ,
    fv_stat FROM gis_local.trxstx_pb25 
    where fv_stat like 'receive%%' and fd_tgltrx <= '{input_value}'
    union all
    SELECT fn_whconsiid,  
    fv_artsizecode,  
    fn_stock ,
    fd_tgltrx  ,
    fv_stat FROM gis_local.trxstx_pb26 
    where fv_stat like 'receive%%' and fd_tgltrx <= '{input_value}'
    ) a
    left join artikel_pb b on a.fv_artsizecode = b.fv_artsizecode
    WHERE 1=1 
    group by fn_whconsiid,fv_barcode, fd_tgltrx 
    """
    with get_engine('imp') as engine:
        df = pd.read_sql(query, engine)
    return df
def df__artikel_pb(tabel):
    query = f'''
        SELECT {tabel} FROM gis_local.artikel_pb
        '''
    with get_engine('imp') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df
def df__artikel_pb_read():
    query='''
    select 
    fv_barcode , fv_artsizecode , fv_catname , fv_brandname , fv_divname , 
    fv_sizename , fm_price , fm_amount , fv_namemark , fv_colorname , 
    fv_fitname , fv_tomname , fv_configname , motif, fv_picture, fv_rating, fm_cogs, fn_articleid, fv_fitting,
    fc_hold
    from (
    select a.fn_articleid, c.fv_brandname, e.fv_divname, d.fv_colorname, g.fv_sizename, k.fv_tomname, 
    b.fv_artsizecode, a.fv_barcode, h. fv_catname, f.fv_namemark, a.fm_price, f.fm_amount, i.fv_configname, 
    l.motif, a.fv_picture, m.fv_configname fv_rating, a.fm_cogs,
    concat(h. fv_catname,' ',j.fv_fitname) as fv_fitname, b.fn_artsizeid, j.fv_fitname fv_fitting, a.fc_hold
    
    from (select * from article_tb) a
    left outer join articlesize_tb b on a.fn_articleid=b.fn_articleid
    left outer join (select * from brand_tb) c on a.fn_brandid=c.fn_brandid
    left outer join (select * from color_tb where fc_hold='F') d on a.fn_colorid=d.fn_colorid
    left outer join (select * from division_tb where fc_hold='F') e on a.fn_divid=e.fn_divid
    left outer join (select * from markdown_tb where fc_hold='F') f on a.fn_markid=f.fn_markid
    left outer join (select * from fitting_tb where fc_hold='F') j on a.fn_fitid=j.fn_fitid
    left outer join (select * from typeofmaterial_tb where fc_hold='F') k on a.fn_tomid=k.fn_tomid
    left outer join (select * from size_tb where fc_hold='F') g on b.fn_sizeid=g.fn_sizeid
    left outer join (select * from category_tb where fc_hold='F') h on a.fn_catid=h.fn_catid
    left outer join config_tb i on i.fn_configid=h.fn_cattypeid
    left outer join (select fn_configid mot,  fv_configname motif from config_tb) l on l.mot=a.fn_motifid
    left outer join config_tb m on m.fn_configid=a.fn_rating
    ) artikel
    '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df_artikel_pb_(colom,distinct=False,where=''):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_local.artikel_pb
        where 1=1 {where}
        '''
    # df = pd.read_sql(query, engine_imp)
    with get_engine('imp') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__customers_pb_(colom,distinct=False,where=''):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'

    query = f'''
        SELECT {distinct} {colom}
        FROM gis_local.customers_pb
        where 1=1 {where}
        '''
    # df = pd.read_sql(query, engine_imp)
    with get_engine('imp') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__customers_pb(tabel):
    query = f'''
        SELECT {tabel} FROM gis_local.customers_pb
        '''
    with get_engine('imp') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df
def df__customers_pb_read():
    query = f'''
        SELECT a.fn_whconsiid, f.fc_hold, f.fv_namecus AS Toko, f.fn_grpcusid,
            c.fv_provincename AS Provinsi, d.fv_cityname AS Kota, f.fn_cusid, f.fc_codecus AS code,
            e.fc_consinetto, e.fv_namegrpcus, h.fv_configname
        FROM warehouseconsi_tb a
        RIGHT JOIN customer_tb f ON a.fn_cusid = f.fn_cusid
        LEFT JOIN province_tb c ON f.fn_provinceid = c.fn_provinceid
        LEFT JOIN city_tb d ON f.fn_cityid = d.fn_cityid
        LEFT JOIN groupcustomer_tb e ON f.fn_grpcusid = e.fn_grpcusid
        LEFT JOIN config_tb h ON f.fn_typestoreid = h.fn_configid
        '''
    
    print(query)
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__customers_pb_read_(colom,distinct=False,where=''):
    if distinct==False:distinct=''
    else :distinct='distinct'
    query = f'''
        SELECT {distinct} {colom}
        FROM warehouseconsi_tb a
        RIGHT JOIN customer_tb f ON a.fn_cusid = f.fn_cusid
        LEFT JOIN province_tb c ON f.fn_provinceid = c.fn_provinceid
        LEFT JOIN city_tb d ON f.fn_cityid = d.fn_cityid
        LEFT JOIN groupcustomer_tb e ON f.fn_grpcusid = e.fn_grpcusid
        LEFT JOIN config_tb h ON f.fn_typestoreid = h.fn_configid 
        where 1=1 {where}
        '''
    print(query)
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df
def df__aging_pb_(colom,distinct=False,where=''):
    if distinct==False:distinct=''
    else :distinct='distinct'
    query_gr = f'''
    SELECT {distinct} {colom} 
    FROM gis_local.aging_gudang_pb 
    where 1=1 {where}
    '''
    with get_engine('imp') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query_gr, engine)
    return df
def df__stockbz_post(colom,distinct=False,where=''):
    if distinct==False:distinct=''
    else :distinct='distinct'
    query_gr = f'''
    SELECT {distinct} {colom} 
    FROM stockarticlebzpostingdtl_tb as a
    left join stockarticlebzpostingmst_tb b on a.fc_nodoc=b.fc_nodoc
    where 1=1 {where} and fn_qtyskrg>0
    '''
    # and fn_bulan=1 and fn_tahun=2026 and fn_qtyskrg>0
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query_gr, engine)
    return df
def df__stockbz_now(colom,distinct=False,where=''):
    if distinct==False:distinct=''
    else :distinct='distinct'
    query_gr = f'''
    SELECT {distinct} {colom} 
    FROM stockarticlebz_tb
    where 1=1 {where} and fn_qty>0
    '''
    # and fn_bulan=1 and fn_tahun=2026 and fn_qtyskrg>0
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query_gr, engine)
    return df

def df__domst_tb(colom,distinct=False,where=''):
    if distinct==False:distinct=''
    else :distinct='distinct'

    if where=='': where=''
    else: where=f'{where}'
    query = f"""
            SELECT {distinct} {colom}
            FROM domst_tb 
            where 1=1 {where}
        """
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df_date_arv = pd.read_sql(query, engine)
    return df_date_arv

def df__user(colom,distinct=False,where=''):
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
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__cus_user():
    query = f'''
        SELECT a.fn_userid, fn_cusid FROM 
    (select fn_userid from user_tb where fn_userlevel=2 and fc_hold='F') a 
    left join (SELECT fn_userid, fn_cusid FROM t_aksesusercust)b on a.fn_userid=b.fn_userid
    where fn_cusid is not null
        '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__pb_penjualan_(year):
    query = f'''SELECT
        fn_whconsiid, fv_artsizecode, fc_monthname, 
        sum(fn_jualpersize) qty, sum(fn_totalpenjualan) 'value' FROM gis_local.pb_penjualan_{year}
        group by fn_whconsiid, fv_artsizecode, fc_monthname
        order by fn_whconsiid, fc_monthname, fv_artsizecode'''
    with get_engine('imp') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
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

def df__penju_bz(year):
    query = f'''SELECT 
        b.fn_idurut, 
        a.fv_nopenjualan, 
        b.fn_whconsiid,
        b.fv_artsizecode, 
        b.fn_jualpersize ,
        b.fn_hargasatuan ,
        b.fn_hargasatuan*b.fn_jualpersize as total_penjualan2,
        DATE_FORMAT(a.fd_tgljual, '%%m-%%d-%%Y') tgltrx,
        b.fn_discid disc_id, 
        z.fv_namemark status_artikel, b.fv_descevent "desc",
        999 as fn_tabel,
        d.fv_namemark 'event'
    FROM 
        t_penjualanbzmst a 
    INNER JOIN 
        t_penjualanbzdtl_goods b ON a.fv_nopenjualan = b.fv_nopenjualan
    LEFT JOIN 
        (SELECT a.fn_discid, b.fv_namemark FROM discount_tb a
            INNER JOIN markdown_tb b ON a.fn_markid = b.fn_markid) d ON b.fn_discid = d.fn_discid
    left join 
        (SELECT c.fv_artsizecode , e.fv_namemark from articlesize_tb c 
            left join article_tb d on c.fn_articleid=d.fn_articleid
            left join markdown_tb e on d.fn_markid=e.fn_markid) z on b.fv_artsizecode=z.fv_artsizecode
    WHERE 
        b.fc_status <> 'F' 
        and a.fc_status <> 'F' 
        and a.fd_tgljual BETWEEN '{year}-01-01 00:00:01' and '{year}-12-31 23:59:59'
    '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__penju2(year):
    query = f'''SELECT 
        a.fv_nopenjualan, 
        DATE_FORMAT(a.fd_tgljual, '%%m-%%d-%%Y') tgltrx,
        DATE_FORMAT(a.fd_tglinput, '%%m-%%d-%%Y') tglinput,
        a.fn_cusid fn_whconsiid,
        b.fv_artsizecode,
        b.fn_hargajualawal price,
        b.fn_disc disc,
        b.fn_hargasatuan ,
        b.fn_jualpersize ,
        b.fn_hargasatuan*b.fn_jualpersize as total_penjualan2,
        a.fv_ketdisc 'EVENT',
        a.fn_userid,
        b.fn_discid disc_id, 
        b.fv_namemark status_artikel, b.fv_descevent "desc",
        d.fv_namedisc 'Disc Name',
        d.fv_namemark 'event',
        b.fn_tabel,
        a.fv_nobonlantai
    FROM 
        t_penjualanmst a 
    INNER JOIN 
        t_penjualandtl_goods b ON a.fv_nopenjualan = b.fv_nopenjualan
    LEFT JOIN 
        (SELECT a.fn_discid, b.fv_namemark, a.fv_namedisc FROM discount_tb a
            INNER JOIN markdown_tb b ON a.fn_markid = b.fn_markid) d ON b.fn_discid = d.fn_discid
    WHERE 
        a.fv_nopenjualan LIKE 'PJ%%' and b.fc_status <> 'F' 
        and a.fc_status <> 'F' 
        and a.fd_tgljual BETWEEN '{year}-02-01 00:00:01' and '{year}-02-28 23:59:59'
    '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__penju_tabel(year):
    query = f'''SELECT 
        fn_whconsiid,
        fv_artsizecode, 
        fn_jualpersize Qty,
        fn_hargasatuan "Harga_Rupiah",
        fn_hargasatuan*fn_jualpersize as "Total_Rupiah",
        DATE_FORMAT(fd_tgltrx, '%%m-%%d-%%Y') tgltrx,
        fv_ornal "Obral/Normal",
        fn_tabel
    FROM 
        pb_penjualan_{year} a 
        where fn_tabel <> 999
    '''
    with get_engine('imp') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__tgl_input_penju(year):
    query = f'''SELECT 
        fv_nopenjualan, 
        fd_tglinput
    FROM 
        t_penjualanmst a 
    WHERE 
        a.fv_nopenjualan LIKE 'PJ%%' 
        and a.fc_status <> 'F' 
        and a.fd_tgljual BETWEEN '{year}-01-01 00:00:01' and '{year}-12-31 23:59:59'
    '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__disc2():
    query = f'''SELECT fv_nopenjualan,artsize_disc2nd, artsize_servisfee, 
    artsize_premi, artsize_affiliate, artsize_voucher  from gis_local.disc2artsize_pb'''
    with get_engine('imp') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df


def df__margin(year):
    query = f'''SELECT * from gis_local.margin{str(year)[-2:]}_pb'''
    with get_engine('imp') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__artsizecode(col='fn_sizeid, fv_artsizecode, fn_artsizeid',stat='read'):
    if stat == 'read':
        query = f'''
        SELECT {col} from articlesize_tb
        '''
        with get_engine('write') as engine:  # Menggunakan context manager dengan with
            df = pd.read_sql(query, engine)
    elif stat == 'write':
        query = f'''
        SELECT {col} from gis_db.articlesize_tb a
        left join gis_db.article_tb b on a.fn_articleid=b.fn_articleid
        where fc_hold ="F"
        '''
        with get_engine('write') as engine:  # Menggunakan context manager dengan with
            df = pd.read_sql(query, engine)
    else:
        query = f'''
        SELECT {col} from artikel_pb
        '''
        with get_engine('imp') as engine:  # Menggunakan context manager dengan with
            df = pd.read_sql(query, engine)
    return df

def df__size():
    query = '''
        SELECT fn_sizeid, fv_sizename FROM size_tb
        '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__tokoawal():
    query = ''' 
    select c.fn_whconsiid from customer_tb a 
    inner join groupcustomer_tb b on a.fn_grpcusid = b.fn_grpcusid
    inner join warehouseconsi_tb c on a.fn_cusid=c.fn_cusid
    where b.fn_typecust='58' 
    '''  
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__tglso(idwhtoko, tglnow):
    """
    Fungsi untuk mendapatkan tanggal Opname terakhir berdasarkan idwhtoko dan tglnow.
    """
    query = '''
            SELECT DISTINCT a.fn_whconsiid, a.fd_tglflag, DATE_FORMAT(a.fd_tglflag, '%%Y-%%m-%%d') AS fd_tglflagIndo, b.fc_hold, a.fc_status
            FROM fl_stockarticleconsipostingmst a
            INNER JOIN warehouseconsi_tb c ON a.fn_whconsiid=c.fn_whconsiid
            INNER JOIN customer_tb b ON b.fn_cusid=c.fn_cusid
            WHERE a.fn_whconsiid = %s AND SUBSTR(a.fd_tglflag, 1, 10) <= %s and fc_status="X"
            ORDER BY a.fd_tglflag DESC LIMIT 1
            '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
                # Mencari tanggal Opname terakhir per tglnow
        df = pd.read_sql(query, engine, params=(idwhtoko, tglnow))
    return df

def df__tglso_():
    """
    Fungsi untuk mendapatkan tanggal Opname terakhir berdasarkan idwhtoko dan tglnow.
    """
    query = '''
            SELECT DISTINCT a.fn_whconsiid, DATE_FORMAT(a.fd_tglflag, '%%Y-%%m-%%d') AS fd_tglflagIndo
            FROM fl_stockarticleconsipostingmst a where fc_status="X"
            '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
                # Mencari tanggal Opname terakhir per tglnow
        df = pd.read_sql(query, engine)
    return df

def df__tglsa(idwhtoko, tglnow):
    """
    Fungsi untuk mendapatkan tanggal Opname terakhir berdasarkan idwhtoko dan tglnow.
    """
    query = '''
    SELECT a.fn_whconsiid, fd_tglinject AS fd_tglflag, DATE_FORMAT(fd_tglinject, '%%Y-%%m-%%d') AS fd_tglflagIndo, b.fc_hold
    FROM saldoawalarticleconsi_tb a
    INNER JOIN warehouseconsi_tb c ON a.fn_whconsiid=c.fn_whconsiid
    INNER JOIN customer_tb b ON b.fn_cusid=c.fn_cusid
    WHERE a.fn_whconsiid = %s AND SUBSTR(fd_tglinject, 1, 10) <= %s
    ORDER BY fd_tglinject DESC LIMIT 1
    '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
                # Mencari tanggal Opname terakhir per tglnow
        df = pd.read_sql(query, engine, params=(idwhtoko, tglnow))
    return df

def df__tglsa_():
    """
    Fungsi untuk mendapatkan tanggal Opname terakhir berdasarkan idwhtoko dan tglnow.
    """
    query = '''
    SELECT DISTINCT a.fn_whconsiid, DATE_FORMAT(fd_tglinject, '%%Y-%%m-%%d') AS fd_tglflagIndo
    FROM saldoawalarticleconsi_tb a
    '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
                # Mencari tanggal Opname terakhir per tglnow
        df = pd.read_sql(query, engine)
    return df

def df_tglsosa(idwhtoko_list_awal,tglEND):
    # Mencari tanggal SO terakhir utnuk masing-masing toko yang masih buka per tanggal yang dipilih
    placeholders = ','.join(['%s'] * len(idwhtoko_list_awal))
    query = f'''SELECT
        t.fn_whconsiid,
        COALESCE(so.tgl_so, sa.tgl_sa) AS fd_tglflag,
        DATE_FORMAT(COALESCE(so.tgl_flag_so, sa.tgl_flag_sa), '%%Y-%%m-%%d') AS fd_tglflagIndo,
        c.fc_hold, so.fc_status, ifnull(fv_stat_so,fv_stat_sa) fv_stat
    FROM warehouseconsi_tb t
    LEFT JOIN (
        SELECT fn_whconsiid, MAX(fd_tglflag) tgl_so, MAX(fd_tglflag) tgl_flag_so ,fc_status, "so" as fv_stat_so
        FROM fl_stockarticleconsipostingmst
        WHERE fc_status='X' AND fd_tglflag <= %s
        GROUP BY fn_whconsiid
    ) so ON so.fn_whconsiid = t.fn_whconsiid
    LEFT JOIN (
        SELECT fn_whconsiid, MAX(fd_tglinject) tgl_sa, MAX(fd_tglinject) tgl_flag_sa, "sa" as fv_stat_sa
        FROM saldoawalarticleconsi_tb
        WHERE fd_tglinject <= %s
        GROUP BY fn_whconsiid
    ) sa ON sa.fn_whconsiid = t.fn_whconsiid
    JOIN customer_tb c ON c.fn_cusid=t.fn_cusid
    WHERE t.fn_whconsiid IN ({placeholders})'''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
                # Mencari tanggal Opname terakhir per tglnow
        return pd.read_sql(query, engine, params=tuple([tglEND,tglEND]+idwhtoko_list_awal))
def df_tglsosa_bz(idwhtoko_list_awal,tglEND):
    # Mencari tanggal SO terakhir utnuk masing-masing toko yang masih buka per tanggal yang dipilih
    placeholders = ','.join(['%s'] * len(idwhtoko_list_awal))
    query = f'''SELECT
        t.fn_whconsiid,
        COALESCE(so.tgl_so, sa.tgl_sa) AS fd_tglflag,
        DATE_FORMAT(COALESCE(so.tgl_flag_so, sa.tgl_flag_sa), '%%Y-%%m-%%d') AS fd_tglflagIndo,
        c.fc_hold, so.fc_status, ifnull(fv_stat_so,fv_stat_sa) fv_stat
    FROM warehouseconsi_tb t
    LEFT JOIN (
        SELECT fn_whconsiid, MAX(fd_tglflag) tgl_so, MAX(fd_tglflag) tgl_flag_so ,fc_status, "so" as fv_stat_so
        FROM fl_stockarticlebzpostingmst
        WHERE fc_status='X' AND fd_tglflag <= %s
        GROUP BY fn_whconsiid
    ) so ON so.fn_whconsiid = t.fn_whconsiid
    LEFT JOIN (
        SELECT fn_whconsiid, MAX(fd_tglinject) tgl_sa, MAX(fd_tglinject) tgl_flag_sa, "sa" as fv_stat_sa
        FROM saldoawalarticleconsi_tb
        WHERE fd_tglinject <= %s
        GROUP BY fn_whconsiid
    ) sa ON sa.fn_whconsiid = t.fn_whconsiid
    JOIN customer_tb c ON c.fn_cusid=t.fn_cusid
    WHERE t.fn_whconsiid IN ({placeholders})'''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
                # Mencari tanggal Opname terakhir per tglnow
        return pd.read_sql(query, engine, params=tuple([tglEND,tglEND]+idwhtoko_list_awal))
    
def df__stock_so(idwhtoko, tglSTR, tglEND):
    query = '''
    select a.fv_artsizecode, a.fn_qtyopname qty
    from t_historysimpanstokopnamecounter a
    inner join fl_stockarticleconsipostingmst b on a.fc_nodoc=b.fc_nodoc
    where b.fn_whconsiid= '%s' AND SUBSTR(b.fd_tglflag, 1, 10) between %s AND %s
    GROUP BY a.fv_artsizecode
    '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
                # Mencari tanggal Opname terakhir per tglnow
        df = pd.read_sql(query, engine, params=(idwhtoko, tglSTR, tglEND))
    return df

def df__stock_sa(idwhtoko, tglEND):
    query = '''
    select a.fv_artsizecode, a.fn_qty qty
    from saldoawalarticleconsi_tb a
    where a.fn_whconsiid= '%s' AND SUBSTR(fd_tglinject, 1, 10) <= %s
    '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
                # Mencari tanggal Opname terakhir per tglnow
        df = pd.read_sql(query, engine, params=(idwhtoko, tglEND))
    return df

def df__transaction(idwhtoko, tglSTR, tglEND, fv_stat):
    
    if fv_stat=='so':
        query_stock = f'''
        select a.fv_artsizecode, a.fn_qtyopname qty
        from t_historysimpanstokopnamecounter a
        inner join fl_stockarticleconsipostingmst b on a.fc_nodoc=b.fc_nodoc
        where b.fn_whconsiid= '{idwhtoko}' AND SUBSTR(b.fd_tglflag, 1, 10) between '{tglSTR}' AND '{tglEND}'
        GROUP BY a.fv_artsizecode
        UNION ALL
        '''
    elif fv_stat=='sa':
        query_stock = f'''                
        select a.fv_artsizecode, a.fn_qty qty
        from saldoawalarticleconsi_tb a
        where a.fn_whconsiid= '{idwhtoko}' AND SUBSTR(fd_tglinject, 1, 10) <= '{tglEND}'
        UNION ALL'''
    else:query_stock=''
    query = f''' 
    SELECT fv_artsizecode, SUM(qty) AS qty
    FROM (
        {query_stock}
    -- Receiving
        SELECT b.fv_artsizecode, IFNULL(SUM(b.fn_qtyterima), 0) AS qty
        FROM t_receivingmst a
        INNER JOIN t_receivingdtl b ON a.fv_noreceiving=b.fv_noreceiving
        WHERE a.fv_noreceiving LIKE %s AND a.fc_status<>'F'
        AND (a.fd_tglreceive BETWEEN %s AND %s)
        GROUP BY b.fv_artsizecode
        UNION ALL 

    -- AdjustIN
        SELECT b.fv_artsizecode, IFNULL(SUM(b.fn_qty), 0) AS qty
        FROM t_adjustconsi_mst a
        INNER JOIN t_adjustconsi_dtl b ON a.fc_nodoc=b.fc_nodoc
        WHERE a.fn_whconsiid= '%s' AND a.fc_status='X' 
        AND b.fc_tipeadj='I' AND b.fn_useridbatal IS NULL
        AND (a.fd_tgladj BETWEEN %s AND %s)
        GROUP BY b.fv_artsizecode
        UNION ALL

    -- Batal Penjualan
        SELECT b.fv_artsizecode, IFNULL(SUM(b.fn_jualpersize), 0) AS qty
        FROM t_penjualanmst a 
        INNER JOIN t_penjualandtl_goods b ON a.fv_nopenjualan=b.fv_nopenjualan
        WHERE a.fn_cusid= '%s' AND (a.fd_tgljual BETWEEN %s AND %s) 
        AND b.fc_status='F'
        GROUP BY b.fv_artsizecode
        UNION ALL
        
    -- Batal Retur
        SELECT b.fv_artsizecode, IFNULL(SUM(b.fn_qtykirim), 0) AS qty
        FROM t_returmst a
        INNER JOIN t_returdtl b ON a.fv_noretur=b.fv_noretur
        WHERE a.fn_whconsiid='%s' AND (a.fd_tgltransaksi BETWEEN %s AND %s)
        AND b.fc_status='F'
        GROUP BY b.fv_artsizecode
        UNION ALL
        
    -- Batal Mutasi
        SELECT b.fv_artsizecode, IFNULL(SUM(b.fn_qtykirim), 0) AS qty
        FROM t_mutasimst a
        INNER JOIN t_mutasidtl b ON a.fv_nomutasi=b.fv_nomutasi
        WHERE a.fn_whconsiid='%s' AND (a.fd_tgltransaksi BETWEEN %s AND %s)
        AND b.fc_status='F'
        GROUP BY b.fv_artsizecode
        UNION ALL
        
    -- Penjualan
        SELECT a.fv_artsizecode, IFNULL(SUM(a.fn_jualpersize), 0) * -1 AS qty
        FROM t_penjualandtl_goods a
        INNER JOIN t_penjualanmst b ON a.fv_nopenjualan=b.fv_nopenjualan
        WHERE b.fn_cusid='%s' AND (b.fd_tgljual BETWEEN  %s AND %s)
        AND a.fv_nopenjualan LIKE 'PJ%%'
        GROUP BY a.fv_artsizecode
        UNION ALL
        
    -- AdjustOUT
        SELECT b.fv_artsizecode, IFNULL(SUM(b.fn_qty), 0) * -1 AS qty
        FROM t_adjustconsi_mst a
        INNER JOIN t_adjustconsi_dtl b ON a.fc_nodoc=b.fc_nodoc
        WHERE a.fn_whconsiid='%s' AND (a.fd_tgladj BETWEEN %s AND %s)
        AND a.fc_status='X' AND b.fc_tipeadj='O' AND b.fn_useridbatal IS NULL
        GROUP BY b.fv_artsizecode
        UNION ALL

    -- Retur
        SELECT a.fv_artsizecode, IFNULL(SUM(a.fn_qtykirim), 0) * -1 AS qty
        FROM t_returdtl a
        INNER JOIN t_returmst b ON a.fv_noretur=b.fv_noretur
        WHERE b.fn_whconsiid='%s' AND a.fv_noretur LIKE 'RT%%'
        AND (b.fd_tgltransaksi BETWEEN %s AND %s)
        GROUP BY a.fv_artsizecode
        UNION ALL
        
    -- Mutasi
        SELECT a.fv_artsizecode, IFNULL(SUM(a.fn_qtykirim), 0) * -1 AS qty
        FROM t_mutasidtl a
        INNER JOIN t_mutasimst b ON a.fv_nomutasi=b.fv_nomutasi
        WHERE b.fn_whconsiid='%s' AND a.fv_nomutasi LIKE 'MT%%'
        AND (b.fd_tgltransaksi BETWEEN %s AND %s)
        GROUP BY a.fv_artsizecode
        )x
    GROUP BY fv_artsizecode
    '''
    
    with get_engine('write') as engine_read:
    # Membuat DataFrame gabungan
        # DataFrame untuk menyimpan hasil
        combined = pd.DataFrame()
        params = (
            f"%/{idwhtoko}", tglSTR, tglEND,
            idwhtoko, tglSTR, tglEND,
            idwhtoko, tglSTR, tglEND,
            idwhtoko, tglSTR, tglEND,
            idwhtoko, tglSTR, tglEND,
            idwhtoko, tglSTR, tglEND,
            idwhtoko, tglSTR, tglEND,
            idwhtoko, tglSTR, tglEND,
            idwhtoko, tglSTR, tglEND
        )
        try:
            # Eksekusi query
            df = pd.read_sql(query, engine_read, params=params)
            # Pastikan nama kolom konsisten
            df.columns = ['fv_artsizecode', 'qty']
            # Gabungkan hasil
            combined = pd.concat([combined, df], ignore_index=True)
        except Exception as e:
            print(f"Error executing query: {query[:100]}... | Error: {e}")
    return combined

def df__transaction_bz(idwhtoko, tglSTR, tglEND, fv_stat):
    
    if fv_stat=='so':
            query_stock = f'''
            select a.fv_artsizecode, a.fn_qtyopname qty
            from t_historysimpanstokopnamebz a
            inner join fl_stockarticlebzpostingmst b on a.fc_nodoc=b.fc_nodoc
            where b.fn_whconsiid= '{idwhtoko}' AND SUBSTR(b.fd_tglflag, 1, 10) between '{tglSTR}' AND '{tglEND}'
            GROUP BY a.fv_artsizecode
            UNION ALL
            '''
    elif fv_stat=='sa':
        query_stock = f'''                
        select a.fv_artsizecode, a.fn_qty qty
        from saldoawalarticleconsi_tb a
        where a.fn_whconsiid= '{idwhtoko}' AND SUBSTR(fd_tglinject, 1, 10) <= '{tglEND}'
        UNION ALL'''
    else:query_stock=''
    query = f''' 
    SELECT fv_artsizecode, SUM(qty) AS qty
    FROM (
        {query_stock}
    -- Receiving
        SELECT b.fv_artsizecode, IFNULL(SUM(b.fn_qtyterima), 0) AS qty
        FROM t_receivingbzmst a
        INNER JOIN t_receivingbzdtl b ON a.fv_noreceiving=b.fv_noreceiving
        WHERE a.fv_noreceiving LIKE %s AND a.fc_status<>'F'
        AND (a.fd_tglreceive BETWEEN %s AND %s)
        GROUP BY b.fv_artsizecode
        UNION ALL 

    -- AdjustIN
        SELECT a.fv_artsizecode,  IFNULL(SUM(fn_qty), 0) as qty FROM stockinbzdtl_tb a
        INNER join stockinbzmst_tb b on a.fc_nodoccounter=b.fc_nodoccounter
        left join stocktambahbzmst_pos c on b.fc_nodoccounter=c.fc_nodoccounter
        where  a.fc_nodoccounter like %s and fd_datebackdate BETWEEN %s AND %s
        and b.fv_nopenjualan is null and a.fv_nopenjualan is null
        and b.fv_noreceiving is null and a.fv_noreceiving is null
        and b.fv_noretur is null and a.fv_noretur is null
        and b.fv_nomutasi is null and a.fv_nomutasi is null
        and b.fv_noopname is null and a.fv_noopname is null
        GROUP BY a.fv_artsizecode
        UNION ALL

    -- Batal Penjualan
        SELECT b.fv_artsizecode, IFNULL(SUM(b.fn_jualpersize), 0) AS qty
        FROM t_penjualanbzmst a 
        INNER JOIN t_penjualanbzdtl_goods b ON a.fv_nopenjualan=b.fv_nopenjualan
        WHERE b.fn_whconsiid= '%s' AND (
            (a.fd_tglbatal IS NOT NULL AND a.fd_tgljual BETWEEN %s AND %s)
            OR 
            (b.fd_tglbatal IS NOT NULL AND b.fd_tgljual BETWEEN %s AND %s)
        )
        AND a.fc_status='F' AND b.fc_status='F' 
        GROUP BY b.fv_artsizecode
        UNION ALL
        
    -- Batal Retur
        SELECT b.fv_artsizecode, IFNULL(SUM(b.fn_qtykirim), 0) AS qty
        FROM t_returbzmst a
        INNER JOIN t_returbzdtl b ON a.fv_noretur=b.fv_noretur
        WHERE a.fn_whconsiid='%s' AND (a.fd_tgltransaksi BETWEEN %s AND %s)
        AND b.fc_status='F'
        GROUP BY b.fv_artsizecode
        UNION ALL
        
    -- Batal Mutasi
        SELECT b.fv_artsizecode, IFNULL(SUM(b.fn_qtykirim), 0) AS qty
        FROM t_mutasibzmst a
        INNER JOIN t_mutasibzdtl b ON a.fv_nomutasi=b.fv_nomutasi
        WHERE a.fn_whconsiid='%s' AND (a.fd_tgltransaksi BETWEEN %s AND %s)
        AND b.fc_status='F'
        GROUP BY b.fv_artsizecode
        UNION ALL
        
    -- Penjualan
        SELECT a.fv_artsizecode, IFNULL(SUM(a.fn_jualpersize), 0) * -1 AS qty
        FROM t_penjualanbzdtl_goods a
        INNER JOIN t_penjualanbzmst b ON a.fv_nopenjualan=b.fv_nopenjualan
        WHERE a.fn_whconsiid='%s' AND (b.fd_tgljual BETWEEN  %s AND %s)
        GROUP BY a.fv_artsizecode
        UNION ALL
        
    -- AdjustOUT
        SELECT a.fv_artsizecode, IFNULL((fn_qty), 0)* -1 AS qty 
        FROM stockoutbzdtl_tb a
        INNER join stockoutbzmst_tb b on a.fc_nodoccounter=b.fc_nodoccounter
        LEFT join stockkeluarbzmst_pos c on a.fc_nodoccounter=c.fc_nodoccounter
        where  a.fc_nodoccounter like %s and fd_datebackdate BETWEEN %s AND %s
        and b.fv_nopenjualan is null and a.fv_nopenjualan is null
        and b.fv_noreceiving is null and a.fv_noreceiving is null
        and b.fv_noretur is null and a.fv_noretur is null
        and b.fv_nomutasi is null and a.fv_nomutasi is null
        and b.fv_noopname is null and a.fv_noopname is null
        GROUP BY a.fv_artsizecode
        UNION ALL

    -- Retur
        SELECT a.fv_artsizecode, IFNULL(SUM(a.fn_qtykirim), 0) * -1 AS qty
        FROM t_returbzdtl a
        INNER JOIN t_returbzmst b ON a.fv_noretur=b.fv_noretur
        WHERE b.fn_whconsiid='%s' 
        AND (b.fd_tgltransaksi BETWEEN %s AND %s)
        GROUP BY a.fv_artsizecode
        UNION ALL
        
    -- Mutasi
        SELECT a.fv_artsizecode, IFNULL(SUM(a.fn_qtykirim), 0) * -1 AS qty
        FROM t_mutasibzdtl a
        INNER JOIN t_mutasibzmst b ON a.fv_nomutasi=b.fv_nomutasi
        WHERE b.fn_whconsiid='%s' 
        AND (b.fd_tgltransaksi BETWEEN %s AND %s)
        GROUP BY a.fv_artsizecode
        )x
    GROUP BY fv_artsizecode
    '''

    with get_engine('write') as engine_read:
    # Membuat DataFrame gabungan
        # DataFrame untuk menyimpan hasil
        combined = pd.DataFrame()
        params = (
            f"%/{idwhtoko}", tglSTR, tglEND,
            f"%/{idwhtoko}", tglSTR, tglEND,
            idwhtoko, tglSTR, tglEND, tglSTR, tglEND,
            idwhtoko, tglSTR, tglEND,
            idwhtoko, tglSTR, tglEND,
            idwhtoko, tglSTR, tglEND,
            f"%/{idwhtoko}", tglSTR, tglEND,
            idwhtoko, tglSTR, tglEND,
            idwhtoko, tglSTR, tglEND,
        )
        try:
            # Eksekusi query
            df = pd.read_sql(query, engine_read, params=params)
            # Pastikan nama kolom konsisten
            df.columns = ['fv_artsizecode', 'qty']
            # Gabungkan hasil
            combined = pd.concat([combined, df], ignore_index=True)
        except Exception as e:
            print(f"Error executing query: {query[:100]}... | Error: {e}")
    return combined

def df__cus():
    """
    Fungsi untuk mendapatkan data customer dari database.
    """
    with get_engine('imp') as engine_imp:  # Menggunakan context manager dengan with
        # Mencari tanggal Opname terakhir per tglnow
        query_customers = '''
        SELECT fc_code , fv_toko , fn_whconsiid,fn_cusid 
        FROM gis_local.customers_pb
        '''
        df = pd.read_sql(query_customers, engine_imp)
    return df

def generate_months_for_year(start_year, end_year):
    months = {}
    month_names = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    
    today = datetime.today()
    current_year, current_month = today.year, today.month  +1 # Mendapatkan tahun dan bulan saat ini
    
    for year in range(start_year, end_year + 1):
        for i, month in enumerate(month_names):
            if year == current_year and (i + 1) > current_month:
                break  # Hentikan jika melewati bulan saat ini
            
            year_suffix = str(year)[-2:]  # Mengambil 2 digit akhir tahun
            start_date = pd.Timestamp(f'{year}-{i+1:02d}-01 00:00:01')  # Awal bulan jam 00:00:01
            end_date = start_date + pd.offsets.MonthEnd(0)  # Akhir bulan (tanpa waktu)
            end_date = pd.Timestamp(end_date.strftime('%Y-%m-%d 23:59:59'))  # Akhir bulan jam 23:59:59
            
            month_str = f'{month}_{year_suffix}'  # Format sesuai penamaan file
            months[month_str] = f"{start_date} to {end_date}"
    
    return months

def get_monthly_sales_data(start_year, end_year):
    with get_engine('write') as engine:
        """Mengambil data penjualan per bulan dari database."""
        months = generate_months_for_year(start_year, end_year)
        sales_data = []
        for month, period in months.items():
            start_date, end_date = period.split(" to ")
            start_date = start_date + " 00:00:00"
            end_date   = end_date + " 23:59:59"
            query = '''
            SELECT 
                a.fn_cusid AS fn_whconsiid,
                UPPER(b.fv_artsizecode) AS fv_artsizecode, 
                IFNULL(SUM(b.fn_jualpersize), 0) AS qty_{month}
            FROM 
                t_penjualanmst a
            INNER JOIN 
                t_penjualandtl_goods b ON a.fv_nopenjualan = b.fv_nopenjualan
            WHERE 
                a.fv_nopenjualan LIKE 'PJ%%' 
                AND b.fc_status <> 'F' 
                AND a.fc_status <> 'F' 
                AND a.fd_tgljual BETWEEN %s AND %s
            GROUP BY 
                fn_whconsiid, fv_artsizecode
            union all
            SELECT 
                b.fn_whconsiid,
                UPPER(b.fv_artsizecode) AS fv_artsizecode,  
                IFNULL(SUM(b.fn_jualpersize), 0) AS qty_{month}
            FROM 
                t_penjualanbzmst a 
            INNER JOIN 
                t_penjualanbzdtl_goods b ON a.fv_nopenjualan = b.fv_nopenjualan
            WHERE 
                b.fc_status <> 'F' 
                and a.fc_status <> 'F' 
                AND a.fd_tgljual BETWEEN %s AND %s
            GROUP BY 
                fn_whconsiid, fv_artsizecode
            '''.format(month=f"{month}")
            
            # Menggunakan parameter untuk mengganti nilai tanggal
            df = pd.read_sql_query(query, engine, params=(start_date, end_date,start_date, end_date))
            sales_data.append(df)
    combined_sales = pd.concat(sales_data, axis=0, ignore_index=True)
    combined_sales = combined_sales.groupby(['fn_whconsiid', 'fv_artsizecode'], as_index=False).sum()
    return combined_sales

def df__retur(start_year):
    query_toko_awal = f''' 

        SELECT fn_whconsiid, a.fv_artsizecode, IFNULL(SUM(a.fn_qtykirim), 0) * -1 AS qty
        FROM t_returdtl a
        INNER JOIN t_returmst b ON a.fv_noretur=b.fv_noretur
        WHERE a.fv_noretur LIKE 'RT%%'
        AND (b.fd_tgltransaksi between '{start_year}-01-01 00:00:00' and '{start_year}-12-31 23:59:59' )
        GROUP BY a.fv_artsizecode
        '''
    with get_engine('write') as engine_read:
        df = pd.read_sql(query_toko_awal, engine_read)
    return df

def get_safe_query(table_name):
    # 1. Cek kolom yang benar-benar ada di database
    check_cols_query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
    """
    # Gunakan engine Anda
    with get_engine('imp') as engine_imp:
        existing_cols = pd.read_sql(check_cols_query, engine_imp)['column_name'].tolist()

    # 2. Definisikan metric
    metrics = ['fn_stock', 'fn_ts', 'fn_qty']
    safe_sum_list = []
    
    # BALIK URUTANNYA: Bulan dulu di luar, baru metrics di dalam
    for i in range(1, 13):
        month = f"{i:02d}"
        for m in metrics:
            col_name = f"{m}_{month}"
            
            if col_name in existing_cols:
                safe_sum_list.append(f"SUM({col_name}) AS {col_name}")
            else:
                # Beri 0 jika kolom tidak ditemukan
                safe_sum_list.append(f"0 AS {col_name}")

    # Gabungkan dengan baris baru agar query rapi saat di-print
    final_cols = ",\n        ".join(safe_sum_list)
    
    return final_cols

def df__sales_thru_24(col,gb='',sum_by=False):
    query = f'''
        SELECT {col} FROM gis_local.sales_thru_pb_24
        {gb}
        '''
    if sum_by==True:
        supp_query=get_safe_query('sales_thru_pb_24')
        query = f'''
        SELECT st.fv_artsizecode,{col} {supp_query},
        sum(fn_stock_at) fn_stock_at, 
        sum(fn_qty_retur) fn_qty_retur, 
        sum(fn_ts_all) fn_ts_all, 
        sum(fn_total_sales_qty) fn_total_sales_qty
        FROM gis_local.sales_thru_pb_24 st
        left join artikel_pb a on st.fv_artsizecode=a.fv_artsizecode
        group by st.fv_artsizecode
        '''
    with get_engine('imp') as engine_imp:
        df = pd.read_sql(query, engine_imp)
    return df

def df__sales_thru_25(col,gb='', sum_by=False):
    query = f'''
        SELECT {col} FROM gis_local.sales_thru_pb_25
        {gb}
        '''
    if sum_by==True:
        supp_query=get_safe_query('sales_thru_pb_25')
        query = f'''
        SELECT st.fv_artsizecode,{col} {supp_query},
        sum(fn_stock_at) fn_stock_at, 
        sum(fn_qty_retur) fn_qty_retur, 
        sum(fn_ts_all) fn_ts_all, 
        sum(fn_total_sales_qty) fn_total_sales_qty
        FROM gis_local.sales_thru_pb_25 st
        left join artikel_pb a on st.fv_artsizecode=a.fv_artsizecode
        group by st.fv_artsizecode
        '''
    with get_engine('imp') as engine_imp:
        df = pd.read_sql(query, engine_imp)
    return df

def df__sales_thru_26(col,gb='', sum_by=False, chunk_size=100000):
    query = f'''
        SELECT {col} FROM gis_local.sales_thru_pb_26
        {gb}
        '''
    if sum_by==True:
        supp_query=get_safe_query('sales_thru_pb_26')
        query = f'''
        SELECT st.fv_artsizecode,{col} {supp_query},
        sum(fn_stock_at) fn_stock_at, 
        sum(fn_qty_retur) fn_qty_retur, 
        sum(fn_ts_all) fn_ts_all, 
        sum(fn_total_sales_qty) fn_total_sales_qty
        FROM gis_local.sales_thru_pb_26 st
        group by st.fv_artsizecode
        '''
    with get_engine('imp') as engine_imp:
        # pd.read_sql dengan chunksize akan mengembalikan Generator/Iterator
        chunks = []
        for chunk in pd.read_sql(query, engine_imp, chunksize=chunk_size):
            chunks.append(chunk)
        
        # Gabungkan semua batch menjadi 1 DataFrame
        df = pd.concat(chunks, ignore_index=True)
    return df

def df__sales_thru_25_count_artikel(stock_cols_str,sales_thru):
    query = f'''
        SELECT fn_whconsiid, fv_barcode, {stock_cols_str}
        FROM gis_local.{sales_thru} a
        left join artikel_pb b on a.fv_artsizecode = b.fv_artsizecode
        '''
    with get_engine('imp') as engine_imp:
        df = pd.read_sql(query, engine_imp)
    return df


def df__sales_(year,tabel):
    query = f'''
        SELECT {tabel} FROM gis_local.pb_penjualan_{year}
        '''
    with get_engine('imp') as engine_imp:
        df = pd.read_sql(query, engine_imp)
    return df
def df__gdmd():
    query='''
    SELECT fv_artsizecode, sum(stock) stock FROM gis_local.stock_gudang_pb 
    where fv_whtypename in ('RETUR','PUSAT','KONSINETTO','QC','FO (BAZAR)') and Artikel is not null
    group by fv_artsizecode
    '''
    with get_engine('imp') as engine_imp:
        df = pd.read_sql(query, engine_imp)
    return df
def df__penju_today():
    today_str = date.today().strftime('%Y-%m-%d')
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
        and (a.fd_tglinput BETWEEN  '{today_str} 00:00:00' and '{today_str} 23:59:00')
    '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__rtt():
    # def trxstx_pb(year):
    today_str = date.today().strftime('%Y-%m-%d')
    with get_engine('write') as engine:
        query=f'''
        SELECT fn_whconsiid, fv_artsizecode, IFNULL(SUM(totalplus), 0) AS stock, tgltrx, stat, no_doc
    FROM (
    select
        SUBSTRING_INDEX(a.fv_noreceiving, '/', -1) AS fn_whconsiid,
        b.fv_artsizecode, 
        b.fn_qtyterima AS totalplus,
        DATE_FORMAT(a.fd_tglreceive, '%%m-%%d-%%Y') tgltrx, 'recieve' as stat,
        a.fv_noreceiving no_doc
        FROM     t_receivingmst a
        INNER JOIN t_receivingdtl b ON a.fv_noreceiving = b.fv_noreceiving
        WHERE a.fc_status <> 'F'
        AND (a.fd_tglsistem BETWEEN '{today_str} 00:00:00' and '{today_str} 23:59:00')
        union all
    SELECT 
        a.fn_whconsiid,
        b.fv_artsizecode, 
        b.fn_qty as totalplus,
        DATE_FORMAT(a.fd_tgladj, '%%m-%%d-%%Y') tgltrx, 'adj_in' as stat,
        a.fc_nodoc no_doc
        FROM 
        t_adjustconsi_mst a 
        INNER JOIN 
        t_adjustconsi_dtl b ON a.fc_nodoc = b.fc_nodoc
        WHERE 
        a.fc_status = 'X' 
        AND b.fc_tipeadj = 'I' 
        AND b.fn_useridbatal IS NULL
        and (a.fd_dateinput between '{today_str} 00:00:00' and '{today_str} 23:59:00')
        union all
    SELECT 
        a.fn_cusid fn_whconsiid,
        b.fv_artsizecode, 
        b.fn_jualpersize as totalplus,
        DATE_FORMAT(a.fd_tgljual, '%%m-%%d-%%Y') tgltrx, 'b_penjualan' as stat,
        a.fv_nopenjualan no_doc
        FROM 
        t_penjualanmst a 
        INNER JOIN 
        t_penjualandtl_goods b ON a.fv_nopenjualan = b.fv_nopenjualan
        WHERE 
        b.fc_status = 'F'
        and (b.fd_tglbatal between '{today_str} 00:00:00' and '{today_str} 23:59:00')
        union all
    SELECT 
        a.fn_whconsiid,
        b.fv_artsizecode, 
        b.fn_qtykirim as totalplus,
        DATE_FORMAT(a.fd_tgltransaksi, '%%m-%%d-%%Y') tgltrx, 'b_retur' as stat,
        a.fv_noretur no_doc
        FROM 
        t_returmst a 
        INNER JOIN 
        t_returdtl b ON a.fv_noretur=b.fv_noretur
        WHERE 
        b.fc_status='F' and 
        (b.fd_tglbatal BETWEEN '{today_str} 00:00:00' and '{today_str} 23:59:00')
        union all
    SELECT 
        a.fn_whconsiid,
        b.fv_artsizecode, 
        (b.fn_stok - b.fn_qtykirim) as totalplus,
        DATE_FORMAT(a.fd_tgltransaksi, '%%m-%%d-%%Y') tgltrx, 'b_retur2' as stat,
        a.fv_noretur no_doc
        FROM 
        t_returmst a 
        INNER JOIN 
        t_returdtl b ON a.fv_noretur=b.fv_noretur
        WHERE 
        b.fn_useridbatal is not null and 
        (b.fd_tglbatal BETWEEN '{today_str} 00:00:00' and '{today_str} 23:59:00')
        union all
    SELECT 
        a.fn_whconsiid,
        b.fv_artsizecode, 
        b.fn_qtykirim as totalplus,
        DATE_FORMAT(a.fd_tgltransaksi, '%%m-%%d-%%Y') tgltrx, 'b_mutasi' as stat,
        a.fv_nomutasi no_doc
        FROM 
        t_mutasimst a 
        INNER JOIN 
        t_mutasidtl b ON a.fv_nomutasi=b.fv_nomutasi
        WHERE 
        b.fc_status='F'
        AND (a.fd_tglbatal BETWEEN '{today_str} 00:00:00' and '{today_str} 23:59:00')
        union all
    SELECT 
        a.fn_whconsiid,
        b.fv_artsizecode, 
        (b.fn_stok - b.fn_qtykirim ) as totalplus,
        DATE_FORMAT(a.fd_tgltransaksi, '%%m-%%d-%%Y') tgltrx, 'b_mutasi2' as stat,
        a.fv_nomutasi no_doc
        FROM 
        t_mutasimst a 
        INNER JOIN 
        t_mutasidtl b ON a.fv_nomutasi=b.fv_nomutasi
        WHERE 
        b.fn_useridbatal is not null
        AND (b.fd_tglbatal BETWEEN '{today_str} 00:00:00' and '{today_str} 23:59:00')
        union all
    SELECT 
        fn_cusid fn_whconsiid,
        b.fv_artsizecode, 
        -1 * b.fn_jualpersize as totalplus,
        DATE_FORMAT(a.fd_tgljual, '%%m-%%d-%%Y') tgltrx, 'penjualan' as stat,
        a.fv_nopenjualan no_doc
        FROM 
        t_penjualanmst a 
        INNER JOIN 
        t_penjualandtl_goods b ON a.fv_nopenjualan = b.fv_nopenjualan
        WHERE 
        a.fv_nopenjualan LIKE 'PJ%%' and b.fd_tglbatal is null
        AND (a.fd_tglinput BETWEEN  '{today_str} 00:00:00' and '{today_str} 23:59:00')
        union all
    SELECT 
        fn_whconsiid,
        b.fv_artsizecode, 
        -1 * b.fn_qty as totalplus,
        DATE_FORMAT(a.fd_tgladj, '%%m-%%d-%%Y') tgltrx, 'adj_out' as stat,
        a.fc_nodoc no_doc
        FROM 
        t_adjustconsi_mst a 
        INNER JOIN 
        t_adjustconsi_dtl b ON a.fc_nodoc = b.fc_nodoc
        WHERE 
        a.fc_status = 'X' 
        AND b.fc_tipeadj = 'O' 
        AND b.fn_useridbatal IS NULL
        AND (a.fd_dateinput BETWEEN '{today_str} 00:00:00' and '{today_str} 23:59:00')
        union all
    SELECT 
        a.fn_whconsiid,
        b.fv_artsizecode, 
        -1 * b.fn_qtykirim as totalplus,
        DATE_FORMAT(a.fd_tgltransaksi, '%%m-%%d-%%Y') tgltrx, 'retur' as stat,
        a.fv_noretur no_doc
        FROM 
        t_returmst a 
        INNER JOIN 
        t_returdtl b ON a.fv_noretur=b.fv_noretur
        WHERE 
        a.fv_noretur LIKE 'RT%%' 
        AND (a.fd_tglinput BETWEEN '{today_str} 00:00:00' and '{today_str} 23:59:00')
        union all
    SELECT 
        a.fn_whconsiid,
        b.fv_artsizecode, 
        -1 * b.fn_qtykirim as totalplus,
        DATE_FORMAT(a.fd_tgltransaksi, '%%m-%%d-%%Y') tgltrx, 'mutasi' as stat,
        a.fv_nomutasi no_doc
        FROM 
        t_mutasimst a 
        inner JOIN 
        t_mutasidtl b ON a.fv_nomutasi=b.fv_nomutasi
        WHERE 
        a.fv_nomutasi LIKE 'MT%%'
        AND (a.fd_tglinput BETWEEN '{today_str} 00:00:00' and '{today_str} 23:59:00')
        union all
    SELECT 
        fn_whconsiid, 
        fv_artsizecode, 
        fn_varian AS totalplus, 
        DATE_FORMAT(fd_tanggal, '%%m-%%d-%%Y') AS tgl, 
        'varian' AS stat,
        fc_nodoc no_doc
        FROM t_historysimpanstokopnamecounter
        WHERE fn_varian <> 0 AND fd_tanggalinject BETWEEN '{today_str} 00:00:00' and '{today_str} 23:59:00'
  
        ) AS subquery
    GROUP BY fn_whconsiid, fv_artsizecode, tgltrx, no_doc;
        '''
    #           union all
    # SELECT 
    #     b.fn_whconsiid,
    #     a.fv_artsizecode, 
    #     a.fn_varian qty
    #     from t_historysimpanstokopnamecounter a
    #     inner join fl_stockarticleconsipostingmst b on a.fc_nodoc=b.fc_nodoc
    #     where fn_varian <> 0 and SUBSTR(b.fd_tglflag, 1, 10) BETWEEN '{today_str} 00:00:00' and '{today_str} 23:59:00'
        df= pd.read_sql(query, engine)
        print(today_str)
        # print(query)
    return df
    
def fetch_data_margin(year):
    with get_engine('imp') as engine:
    # Fetch data for the given year        
        query_artimp = '''
        SELECT fv_barcode, fv_artsizecode 
        FROM gis_local.artikel_pb
        '''
        df_artimp = pd.read_sql(query_artimp, engine)
    with get_engine('write') as engine:
        query_trx = f'''
        SELECT fn_idurut, fn_whconsiid, fd_tgljual fd_tgltrx, fv_artsizecode 
        FROM t_penjualandtl_goods where fd_tgljual BETWEEN '{year}-01-01' AND '{year}-12-31'
        '''
        df_trx = pd.read_sql(query_trx, engine)
        
        query_artred = '''
        SELECT fn_catid, fv_barcode 
        FROM article_tb
        '''
        df_artred = pd.read_sql(query_artred, engine)
        
        query_atrxu = f'''
        SELECT fn_idurut, markid2, fn_markid 
        FROM (
            SELECT fn_idurut, fn_markid markid2, fn_idevent 
            FROM t_penjualandtl_goods 
            WHERE fd_tgljual BETWEEN '{year}-01-01' AND '{year}-12-31'
        ) a 
        LEFT JOIN (SELECT fn_idevent, fn_discid FROM event_tb) b ON a.fn_idevent = b.fn_idevent
        LEFT JOIN (SELECT fn_discid, fn_markid FROM discount_tb) c ON b.fn_discid = c.fn_discid
        '''
        df_atrxu = pd.read_sql(query_atrxu, engine)
        
        query_mrgincat = f'''
        SELECT fn_whconsiid, fn_catid, fn_markid, fd_begindate, fd_enddate, fn_margin 
        FROM (
            SELECT * 
            FROM margincat_tb 
            WHERE fd_begindate > '{year-1}-12-31 00:00:00' AND fd_enddate < '{year+1}-01-01 00:00:00'
        ) a
        LEFT JOIN (SELECT fc_code, fn_cusid FROM margincatstore_tb) b ON a.fc_code = b.fc_code
        LEFT JOIN (SELECT fn_cusid, fn_whconsiid FROM warehouseconsi_tb) c ON b.fn_cusid = c.fn_cusid
        '''
        df_mrgincat = pd.read_sql(query_mrgincat, engine)
        
        query_mrgin = f'''
        SELECT fn_whconsiid, fn_markid, fd_effectivedate, fn_margin 
        FROM (
            SELECT fn_marginid, fd_effectivedate, fn_markid, fn_margin 
            FROM margin_tb 
            WHERE fd_effectivedate BETWEEN '{year}-01-01' AND '{year}-12-31'
        ) a
        LEFT JOIN (SELECT fn_marginid, fn_cusid FROM margindtl_tb) b ON a.fn_marginid = b.fn_marginid
        LEFT JOIN (SELECT fn_cusid, fn_whconsiid FROM warehouseconsi_tb) c ON b.fn_cusid = c.fn_cusid
        '''
        df_mrgin = pd.read_sql(query_mrgin, engine)
        
    return df_trx, df_artimp, df_artred, df_atrxu, df_mrgincat, df_mrgin

def df__sls_putus_disc():
    with get_engine('write') as engine:
        query='''
        SELECT fc_nodoc, fn_totalqty, fm_totaldisc FROM salesinvmstnew_tb
        '''
        df_simst= pd.read_sql(query, engine)
        query='''
        SELECT fc_nodoc, sum(fm_totdisc) fm_totdisc FROM salesinvdtlnew_tb
        group by fc_nodoc
            '''
        df_sidtl= pd.read_sql(query, engine)
    return df_simst, df_sidtl

def df__sales_invoice():
    with get_engine('write') as engine:
        query='''
        SELECT a.fc_nodoc, a.fn_cusid, a.fd_invdate tgltrx, b.fv_artsizecode, fn_qty, fm_netto hargajual, fm_dpp/fn_qty dpp  FROM salesinvdtlnew_tb b
        inner join salesinvmstnew_tb a on a.fc_nodoc=b.fc_nodoc
        '''
        df_si= pd.read_sql(query, engine)
    return df_si

def df__sales_bz():
    with get_engine('write') as engine:
        query = f'''SELECT 
        b.fn_idurut, 
        a.fv_nopenjualan, 
        a.fn_cusid ,
        b.fv_artsizecode, 
        b.fn_jualpersize,
        b.fn_hargasatuan,
        b.fn_hargasatuan*b.fn_jualpersize as total_penjualan,
        DATE_FORMAT(a.fd_tgljual, '%%m-%%d-%%Y') tgltrx,
        b.fn_discid disc_id, 
        c.fv_namemark status_artikel, b.fv_descevent "desc",
        d.fv_namemark 'event'
        FROM t_penjualanbzmst a
        INNER JOIN t_penjualanbzdtl_goods b ON a.fv_nopenjualan = b.fv_nopenjualan 
        LEFT JOIN 
        (SELECT a.fn_discid, b.fv_namemark FROM discount_tb a
            INNER JOIN markdown_tb b ON a.fn_markid = b.fn_markid) d ON b.fn_discid = d.fn_discid
        left join 
            (SELECT b.fv_articlecode, a.fv_namemark FROM markdown_tb a
            INNER JOIN article_tb b ON a.fn_markid = b.fn_markid) c on b.fv_articlecode=c.fv_articlecode
        where a.fv_nopenjualan like 'BZ%%' and b.fc_status <> "F" and a.fc_status <> "F"
        '''
        df= pd.read_sql(query, engine)
    return df
 
def df__sales_bz_disc2nd():
    with get_engine('write') as engine:
        query='''select a.fv_nopenjualan, fn_totalqty, fn_totalhargadiskon from
        (SELECT fv_nopenjualan, fn_potonganharga fn_totalhargadiskon FROM t_penjualanbzmst
        where fn_potonganharga <> 0) a 
        left join (SELECT fv_nopenjualan, sum(fn_jualpersize) fn_totalqty FROM t_penjualanbzdtl_goods
        group by fv_nopenjualan) b on a.fv_nopenjualan=b.fv_nopenjualan
        '''
        df= pd.read_sql(query, engine)
    df['disc2nd'] = np.where(
    df['fn_totalqty'] != 0, 
    np.ceil(df['fn_totalhargadiskon'] / df['fn_totalqty']), 
    0)
    # Mendapatkan nama kolom pertama dan terakhir
    col0 = df.columns[0]
    col_last = df.columns[-1]

    # Membuat DataFrame baru hanya dengan kolom pertama dan terakhir
    df = df[[col0, col_last]]
    return df

def df__sales_consi_disc2nd():
    with get_engine('write') as engine:
        query='''select a.fv_nopenjualan, fn_potonganharga, fn_totalqty qty from
        (SELECT fv_nopenjualan, fn_potonganharga FROM t_penjualanmst
        where fn_potonganharga <> 0) a 
        left join (SELECT fv_nopenjualan, sum(fn_jualpersize) fn_totalqty FROM t_penjualandtl_goods
        group by fv_nopenjualan) b on a.fv_nopenjualan=b.fv_nopenjualan
        '''
        df= pd.read_sql(query, engine)
    return df

def df__sales_online_disc2nd():
    with get_engine('write') as engine:
        query='''select a.fv_nopenjualan, servisfee, premi, affiliate, voucher, fn_totalqty qty from
        (SELECT fv_nopenjualan, fm_asuransi servisfee, fm_ongkir premi, fm_voucher affiliate, fm_lainlain voucher FROM t_penjualanpotongan
        ) a 
        left join (SELECT fv_nopenjualan, sum(fn_jualpersize) fn_totalqty FROM t_penjualandtl_goods
        group by fv_nopenjualan) b on a.fv_nopenjualan=b.fv_nopenjualan
        '''
        df= pd.read_sql(query, engine)
    return df

def df__sales_ptsbz():
    with get_engine('write') as engine:
        query = f'''SELECT 
            fc_codecus,
            fv_artsizecode,
            SUM(total_qty) AS total_qty,
            SUM(total_penjualan) AS total_penjualan,
            tgltrx,
            disc_id,
            event, harga
        FROM (
            SELECT 
                e.fc_codecus, 
                b.fv_artsizecode, 
                SUM(b.fn_jualpersize) AS total_qty,
                SUM(b.fn_hargasatuan * b.fn_jualpersize) AS total_penjualan,
                DATE_FORMAT(a.fd_tgljual, '%%m-%%d-%%Y') AS tgltrx,
                b.fn_discid AS disc_id, 
                d.fv_namemark AS event,
                b.fn_hargasatuan harga
            FROM t_penjualanbzmst a
            INNER JOIN t_penjualanbzdtl_goods b ON a.fv_nopenjualan = b.fv_nopenjualan 
            LEFT JOIN (
                SELECT a.fn_discid, b.fv_namemark 
                FROM discount_tb a
                INNER JOIN markdown_tb b ON a.fn_markid = b.fn_markid
            ) d ON b.fn_discid = d.fn_discid
            LEFT JOIN customer_tb e ON a.fn_cusid = e.fn_cusid
            WHERE a.fv_nopenjualan LIKE 'BZ%%' 
              AND b.fc_status <> 'F' 
              AND a.fc_status <> 'F'
            GROUP BY e.fc_codecus, b.fv_artsizecode, tgltrx, b.fn_discid, d.fv_namemark
        
            UNION ALL
        
            SELECT
                e.fc_codecus, 
                b.fv_artsizecode, 
                SUM(b.fn_qty) AS total_qty,
                SUM(b.fm_netto * b.fn_qty) AS total_penjualan,
                DATE_FORMAT(a.fd_invdate, '%%m-%%d-%%Y') AS tgltrx,
                0 AS disc_id, 
                '' AS event, fm_netto harga
            FROM salesinvdtlnew_tb b
            INNER JOIN salesinvmstnew_tb a ON a.fc_nodoc = b.fc_nodoc
            LEFT JOIN customer_tb e ON a.fn_cusid = e.fn_cusid
            GROUP BY e.fc_codecus, b.fv_artsizecode, tgltrx
        ) AS subquery
        GROUP BY fc_codecus, fv_artsizecode, tgltrx, disc_id, event;
        '''
        df= pd.read_sql(query, engine)
    return df
def df__sales_pmse():
    with get_engine('write') as engine:
        query = f'''
            SELECT 
                e.fc_codecus, 
                b.fv_artsizecode, 
                SUM(b.fn_jualpersize) AS total_qty,
                SUM(b.fn_hargasatuan * b.fn_jualpersize) AS total_penjualan,
                DATE_FORMAT(a.fd_tgljual, '%%m-%%d-%%Y') AS tgltrx,
                b.fn_discid AS disc_id, 
                d.fv_namemark AS event
            FROM t_penjualanmst a
            INNER JOIN t_penjualandtl_goods b ON a.fv_nopenjualan = b.fv_nopenjualan 
            LEFT JOIN (
                SELECT a.fn_discid, b.fv_namemark 
                FROM discount_tb a
                INNER JOIN markdown_tb b ON a.fn_markid = b.fn_markid
            ) d ON b.fn_discid = d.fn_discid
            LEFT JOIN customer_tb e ON a.fn_cusid = e.fn_cusid
            WHERE fc_codecus like "PMSE%%"
              AND b.fc_status <> 'F' 
              AND a.fc_status <> 'F'
            GROUP BY e.fc_codecus, b.fv_artsizecode, tgltrx, b.fn_discid, d.fv_namemark
            '''
        df= pd.read_sql(query, engine)
    return df
def df__otsdo():
    today = date.today()
    year = today.year
    with get_engine('write') as engine:
        query= f'''SELECT 
        a.fc_nodoc ,fv_artsizecode, sum(fn_qty) fn_qty, fd_date, b.fc_status, fn_whconsiid, b.fd_dateinput
        FROM dodtl_tb a 
        inner join domst_tb b on a.fc_nodoc=b.fc_nodoc
        inner join t_ordermst c on b.fc_noreff=fv_noorder
        left join t_receivingmst d on b.fc_nodoc=d.fc_nosj
        left join warehouseconsi_tb e on c.fn_cusid=e.fn_cusid
        where  fv_noreceiving is null and fd_date > '{year}-01-01 00:00:00'and b.fc_status="F" 
        group by fn_whconsiid, fv_artsizecode, fd_date 
        '''
        df= pd.read_sql(query, engine)
    return df

def df__otsdo_rt(tglinput):
    with get_engine('write') as engine:
        query= f'''SELECT distinct 
        fn_cusid, fv_artsizecode, fn_userid,
        a.fc_nodoc "No DO",
        fd_date "Tgl Transaksi",
        b.fd_dateinput "Tgl Pengajuan",
        sum(fn_qty) OstDO,  b.fc_status 
        FROM gis_db.dodtl_tb a 
        inner join gis_db.domst_tb b on a.fc_nodoc=b.fc_nodoc
        inner join gis_db.t_ordermst c on b.fc_noreff=fv_noorder
        left join gis_db.t_receivingmst d on b.fc_nodoc=d.fc_nosj
        where fv_noreceiving is null and b.fd_dateinput > '{tglinput} 00:00:00' and b.fc_status="F" 
        group by a.fc_nodoc, fn_cusid, fv_artsizecode, fd_date 
        '''
        df= pd.read_sql(query, engine)
    return df

def df__otsdo_del(tglmax,tglmin):
    with get_engine('write') as engine:
        query= f'''SELECT 
        a.fc_nodoc ,fv_artsizecode
        FROM gis_db.dodtl_tb a 
        inner join gis_db.domst_tb b on a.fc_nodoc=b.fc_nodoc
        inner join gis_db.t_ordermst c on b.fc_noreff=fv_noorder
        left join gis_db.t_receivingmst d on b.fc_nodoc=d.fc_nosj
        where  fv_noreceiving is null 
        and b.fd_dateinput between '{tglmin}' and '{tglmax}' and b.fc_status="F" 
        group by fc_nodoc, fn_cusid, fv_artsizecode, fd_date 
        '''
        df_del= pd.read_sql(query, engine)
    with get_engine('imp') as engine:
        query= f'''SELECT 
        pk_id
        FROM gis_local.dt_OSDO 
        where fd_dateinput between '{tglmin}' and '{tglmax}' 
        '''
        df_main= pd.read_sql(query, engine)
    return df_del, df_main

def df__otsdm():
    today = date.today()
    year = today.year
    with get_engine('write') as engine:
        query= f'''SELECT 
            a.fc_nodoc ,fv_artsizecode, sum(fn_qty) fn_qty, fd_date, b.fc_status, fn_whconsiid, fn_whconsiid2
            FROM dodtl_tb a 
            inner join domst_tb b on a.fc_nodoc=b.fc_nodoc
            inner join t_mutasimst c on b.fc_noreff=c.fv_nomutasi
            left join t_receivingmst d on b.fc_nodoc=d.fc_nosj
            where  fv_noreceiving is null and fd_date > '{year}-01-01 00:00:00'
            and b.fc_status="F" 
            group by fn_whconsiid, fv_artsizecode, fd_date  
        '''
        df= pd.read_sql(query, engine)
    return df

def df__otsdmdo_rt():
    with get_engine('write') as engine:
        query= f'''SELECT 
            fv_artsizecode, sum(fn_qty) fn_qty
            FROM dodtl_tb a 
            inner join domst_tb b on a.fc_nodoc=b.fc_nodoc
            left join t_receivingmst d on b.fc_nodoc=d.fc_nosj
            where  fv_noreceiving is null and b.fc_status="F" 
            and fc_noreff not like "%%BATAL%%" and fc_noreff not like "SO%%"
            and fd_date > "2026-01-01"
            group by fv_artsizecode  
        '''
        df= pd.read_sql(query, engine)
    return df

def df__ots_retur_gd():
    with get_engine('write') as engine:
        query= f'''SELECT distinct fv_artsizecode,sum(fn_qtykirim) fn_qty FROM 
            (SELECT fv_noretur FROM gis_db.t_returmst a
            left join  gis_db.returnmst_tb c on a.fv_noretur=c.fc_nodocstore where c.fc_nodoc is null) a  
            left join gis_db.t_returdtl b on a.fv_noretur=b.fv_noretur 
            group by fv_artsizecode
        '''
        df_rtr= pd.read_sql(query, engine)
    return df_rtr

def df__otsob():
    query='''SELECT distinct  
        fn_cusid, fv_artsizecode, fn_userid,
        b.fv_noorder "No OB",
        fd_tglorder "Tgl Pengajuan", 
        fd_tgldelivery "Tgl On Store",
        sum(fn_orderpersize) OstOB,
          fv_keteranganumum "Keterangan Umum"  FROM gis_db.t_orderdtl_goods_tmp a 
        inner join gis_db.t_ordermst b on a.fv_noorder=b.fv_noorder
        left join gis_db.domst_tb c on b.fv_noorder=c.fc_noreff
        where fc_nodoc is null
        group by fn_cusid, fv_artsizecode'''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__artikel_status():
    query='''select fn_articleid, fv_status from gis_db.articlestatus_tb'''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__whtype_name():
    query='''SELECT fn_whtypeid, a.fn_whid, fv_whtypecode, fv_whtypename, fv_whcode FROM gis_db.warehousetype_tb a 
            inner join gis_db.warehouse_tb b on a.fn_whid=b.fn_whid
            where a.fc_hold="F" and b.fc_hold="F";'''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__aging_gudang():
    query_gr = '''
    SELECT a.fc_nodoc, fd_arrivaldate, fv_nopo, fn_articleid, fn_qty FROM grdtl_tb a
    inner join grmst_tb b on a.fc_nodoc = b.fc_nodoc
    '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query_gr, engine)
    return df

def df__stock_today_all_store():
    input_value = datetime.now().strftime("%Y-%m-%d")   
    year = datetime.now().year
    sales_thru=f'sales_thru_pb_{str(year)[-2:]}'
    trxstx = f"trxstx_pb{str(year)[-2:]}"
    stock = f"fn_stock_{input_value[5:7]}"

    main_query = f"""
        SELECT stock_backdate.fn_whconsiid,stock_backdate.fv_artsizecode,fv_barcode, fv_sizename, a.fv_configname,sum(stock) stocks
        FROM (
            SELECT fn_whconsiid, {stock} AS stock, b.fv_artsizecode 
            FROM gis_local.{sales_thru} b
            LEFT JOIN artikel_pb a ON b.fv_artsizecode = a.fv_artsizecode
            where 1=1
            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, b.fv_artsizecode 
            FROM gis_local.{trxstx} b
            LEFT JOIN artikel_pb a ON b.fv_artsizecode = a.fv_artsizecode
            WHERE fd_tgltrx BETWEEN '{str(year)[-2:]}-{input_value[5:7]}-01' AND '{str(year)[-2:]}-{input_value[5:10]}'
            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
            FROM gis_local.trxrt_pb
            WHERE fd_tgltrx <= '{str(year)[-2:]}-{input_value[5:10]}'  and (fv_stat = 'adj_out' or fv_stat = 'adj_in')
            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, fv_artsizecode 
            FROM gis_local.trxrt_pb
            WHERE fd_tgltrx BETWEEN '{str(year)[-2:]}-{input_value[5:7]}-01' AND '{str(year)[-2:]}-{input_value[5:10]}'
            UNION ALL
            SELECT fn_whconsiid, fn_stock AS stock, b.fv_artsizecode 
            FROM gis_local.Stock_varian_{str(year)[-2:]} b
            LEFT JOIN artikel_pb a ON b.fv_artsizecode = a.fv_artsizecode
            WHERE fd_tgl BETWEEN '{str(year)[-2:]}-{input_value[5:7]}-01' AND '{str(year)[-2:]}-{input_value[5:10]}'
        ) stock_backdate
        LEFT JOIN artikel_pb a ON a.fv_artsizecode = stock_backdate.fv_artsizecode
        LEFT JOIN customers_pb b ON stock_backdate.fn_whconsiid = b.fn_whconsiid
        WHERE 1=1 
        group by stock_backdate.fn_whconsiid,stock_backdate.fv_artsizecode
        """
    with get_engine('imp') as engine:
        df = pd.read_sql(main_query, engine)
    return df

def df__stock_wh():
    query = f"""
            SELECT 
                fv_whtypename, fv_artsizecode, fn_qty 
            FROM gis_db.stockarticle_tb a 
            LEFT JOIN gis_db.warehousetype_tb b 
                ON a.fn_whtypeid = b.fn_whtypeid
            WHERE 1=1 
            AND fn_qty > 0
        """
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df
def df__fstock_wh():# ambil list barcode unik dari df
    query= f'''
    SELECT 
    distinct fv_whtypename, fv_artsizecode, fn_qty FROM (
        SELECT 
            distinct fn_whid, fv_artsizecode, sum(fn_orderingpersize * -1) fn_qty
            FROM gis_db.t_orderdtl_goods_tmp_pos 
            where fv_noorder not like 'OB%%' and fv_noorder not like 'SO%%'  
            and fc_status='T' and fn_whid not in (0,1)
            group by fn_whid, fv_artsizecode
	union all 
        SELECT 
            distinct fn_whid, fv_artsizecode, sum(fn_orderingpersize * -1) fn_qty FROM gis_db.t_orderdtl_goods_tmp
            where fc_status ="T"  and fn_whid not in (0,1)
            group by fn_whid, fv_artsizecode) a 
    left join gis_db.warehousetype_tb b on a.fn_whid=b.fn_whtypeid
        '''
    # # printol(query)
    # df = pd.read_sql(query, engine_write)
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df
def df__onbook_stock(df=None, 
                     type_show='all',
                     subid=False, 
                     fn_nodraf=False, 
                     fn_whconsiid=False,
                     list_from=None, set_minus=True):
    
    # type_show='all'
    # set_minus=True
    if type_show == 'all': whid = ''
    elif type_show == 'pst': whid = 'and fn_whconsiid in (0)'
    elif type_show == 'konsi': whid = 'and fn_whconsiid in (9999)'
    else: whid = 'and fn_whconsiid in (0,9999)'


    # 2. Setup SELECT dan GROUP BY secara dinamis untuk mencegah error agregasi
    select_cols = ['fn_whconsiid as fv_whtypename', 'fv_artsizecode']
    group_cols = ['fn_whconsiid', 'fv_artsizecode']

    whconsi_list = '' 
    qty_calc = 'sum(fn_on_book * -1)' if set_minus else 'sum(fn_on_book)'
    select_cols.append(f'{qty_calc} as fn_qty')

    select_clause = ", ".join(select_cols)
    group_clause = ", ".join(group_cols)

    # 3. Filter dokumen disatukan dalam satu variabel agar selalu konsisten
    doc_filter = 'and (fv_nodraft like "BK%%" or fv_nodraft like "OB%%")'

    # 4. Logic penentuan list_artsize
    list_artsize_str = ""

    # 5. Final Query - Menggunakan doc_filter dan group_clause yang sudah diperbaiki
    query = f'''
        SELECT {select_clause}
        FROM gis_local.t_draftdtl
        WHERE fc_status='X' {doc_filter} {list_artsize_str} {whid} {whconsi_list}
        GROUP BY {group_clause}
    '''
    with get_engine('imp') as engine:
        df_final = pd.read_sql(query, engine)

    # if df_final.empty:
    #     df_final
    # else:
    #     df_final['fv_whtypename'] = np.where(
    #                         df_final['fv_whtypename'] == 0, 'PUSAT',
    #                         np.where(df_final['fv_whtypename'] == 9999, 'KONSINETTO', df_final['fv_whtypename']))
    
    if df_final.empty:
        return df_final
        
    df_final['fv_whtypename'] = np.where(
                        df_final['fv_whtypename'] == 0, 'PUSAT',
                        np.where(df_final['fv_whtypename'] == 9999, 'KONSINETTO', df_final['fv_whtypename']))
    return df_final
def df__aging_gudang_pb():
    query = f"""
            SELECT 
                fn_articleid, fn_hari_fdo fn_hari, fn_hari "last receive gudang"
            FROM gis_local.aging_gudang_pb 
        """
    with get_engine('imp') as engine:  # Menggunakan context manager dengan with
        df_aging = pd.read_sql(query, engine)
    return df_aging

def df__tracking_do():
    query = f'''SELECT distinct
                    b.fc_nodoc AS noexp,
                    d.fn_cusid AS fn_cusid,
                    c.fc_nodoc AS nodo,
                    c2.fv_noorder AS noob,
                    b.fn_idexp AS fn_idexp,
                    a2.fv_nameexp AS fv_nameexp,
                    b.fd_pickupdate AS fd_pickupdate,
                    c2.fv_namacust AS fv_namacust,
                    d.fv_desc AS fv_nosi,
                    b.fn_idexpadd AS fn_idexpadd,
                    e.fv_noreceiving AS fv_noreceiving,
                    e.fd_tglreceive AS fd_tglreceive,
                    e.fd_tglsistem AS fd_tglsistem,
                    c.fc_status AS fc_status,
                    0 as fn_whconsiid,
                    fn_whconsiid as fn_whconsiid2,
                    z.fv_nosi fv_nosi_price
                FROM expedition_tb a2

                -- Join utama ekspedisi
                LEFT JOIN expeditiondeliverymst_tb b ON a2.fn_idexp = b.fn_idexp
                LEFT JOIN expeditiondeliverysubdtl_tb a ON b.fc_nodoc = a.fc_nodoc

                -- Join ke DO dan Order
                LEFT JOIN domst_tb c ON c.fc_nodoc = a.fc_nodo
                LEFT JOIN t_ordermst c2 ON c2.fv_noorder = c.fc_noreff
                -- Join detail ekspedisi
                LEFT JOIN expeditiondeliverydtl_tb d ON d.fc_nodoc = a.fc_nodoc AND d.fv_number = a.fv_number
                LEFT JOIN warehouseconsi_tb c3 ON d.fn_cusid = c3.fn_cusid
                -- Join ke Receiving menggunakan UNION (untuk Normal dan BZ)
                LEFT JOIN (
                    SELECT fv_noreceiving, fd_tglreceive, fd_tglsistem, fc_nosj FROM t_receivingmst
                    UNION ALL 
                    SELECT fv_noreceiving, fd_tglreceive, fd_tglsistem, fc_nosj FROM t_receivingbzmst
                ) e ON c.fc_nodoc = e.fc_nosj
                LEFT JOIN expeditiondeliveryprice_tb z ON a.fc_nodoc = z.fc_nodoc AND a.fv_number = z.fv_number
                
                -- Filter
                WHERE b.fd_pickupdate > '2024-01-01'
                ORDER BY b.fd_pickupdate DESC;
    '''
    with get_engine('write') as engine:  
        df_do=pd.read_sql(query, engine)
    return df_do

def df__tracking_rm():
    query = f'''SELECT distinct
                    b.fc_nodoc AS noexp,
                    CAST(d.fn_cusid AS SIGNED) AS fn_cusid,
                    c3.fc_nodoc AS nodo,
                    c.fc_nodoc AS noob,
                    b.fn_idexp AS fn_idexp,
                    a2.fv_nameexp AS fv_nameexp,
                    b.fd_pickupdate AS fd_pickupdate,
                    c2.fv_namecus AS fv_namacust,
                    d.fv_desc AS fv_nosi,
                    b.fn_idexpadd AS fn_idexpadd,
                    e.fv_noreceiving AS fv_noreceiving,
                    e.fd_tglreceive AS fd_tglreceive,
                    e.fd_tglsistem AS fd_tglsistem,
                    c.fc_status AS fc_status,
                    fn_whconsiid,
                    fn_whconsiid2,
                    z.fv_nosi fv_nosi_price
                FROM gis_db.expedition_tb a2

                -- Join utama ekspedisi
                LEFT JOIN gis_db.retmutexpeditionmst_tb b ON a2.fn_idexp = b.fn_idexp
                LEFT JOIN gis_db.retmutexpeditionsubdtl_tb a ON b.fc_nodoc = a.fc_nodoc

                -- Join ke DO dan Order
            -- LEFT JOIN gis_db.domst_tb c ON c.fc_nodoc = a.fc_nodo
                LEFT JOIN (
                    SELECT fv_nomutasi fc_nodoc, fn_whconsiid, fc_status, fn_whconsiid2 FROM gis_db.t_mutasimst
                    UNION ALL 
                    SELECT fv_nomutasi fc_nodoc, fn_whconsiid, fc_status, fn_whconsiid2 FROM gis_db.t_mutasibzmst
                    UNION ALL 
                    SELECT fv_noretur fc_nodoc, fn_whconsiid, fc_status,0 as fn_whconsiid2 FROM gis_db.t_returmst
                    UNION ALL 
                    SELECT fv_noretur fc_nodoc, fn_whconsiid, fc_status,0 as fn_whconsiid2 FROM gis_db.t_returbzmst
                ) c ON c.fc_nodoc = a.fc_noreff
                LEFT JOIN gis_db.domst_tb c3 ON c.fc_nodoc = c3.fc_noreff
            -- LEFT JOIN gis_db.t_ordermst c2 ON c2.fv_noorder = c.fc_noreff

                -- Join detail ekspedisi
                LEFT JOIN gis_db.retmutexpeditiondtl_tb d ON d.fc_nodoc = a.fc_nodoc AND d.fv_number = a.fv_number
                LEFT JOIN customer_tb c2 ON d.fn_cusid = c2.fn_cusid
                -- Join ke Receiving menggunakan UNION (untuk Normal dan BZ)
                LEFT JOIN (
                    SELECT fv_noreceiving, fd_tglreceive, fd_tglsistem, fv_noso FROM gis_db.t_receivingmst
                    UNION ALL 
                    SELECT fv_noreceiving, fd_tglreceive, fd_tglsistem, fv_noso FROM gis_db.t_receivingbzmst
                    UNION ALL 
                    SELECT fc_nodoc fv_noreceiving,fd_arrivaldate fd_tglreceive,
                    fd_dateinput fd_tglsistem,fc_nodocstore fv_noso FROM gis_db.returnmst_tb
                ) e ON c.fc_nodoc = e.fv_noso
                
                LEFT JOIN gis_db.retmutexpeditionprice_tb z ON a.fc_nodoc = z.fc_nodoc AND a.fv_number = z.fv_number
                -- Filter
                WHERE b.fd_pickupdate > '2026-01-01'
                ORDER BY b.fd_pickupdate DESC;
    '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
                # Mencari tanggal Opname terakhir per tglnow
        df_rm=pd.read_sql(query, engine)
    return df_rm

def df__detail_tracking(year):
    fd_date=f"'{year}-01-01' and '{year}-12-31'"
    query = f'''SELECT fc_nodoc as fv_nodoc,fn_whconsiid, send.fv_artsizecode,fn_qty,fn_qtyterima from
        (
            SELECT a.fc_nodoc,SUBSTRING_INDEX(a.fc_nodoc, '/', -1) AS fn_whconsiid, fv_artsizecode,fn_qty 
            FROM gis_db.dodtl_tb a 
            inner join gis_db.domst_tb b on a.fc_nodoc=b.fc_nodoc
            where fd_date between {fd_date}
            union all 
            SELECT a.fv_noretur fc_nodoc,SUBSTRING_INDEX(a.fv_noretur, '/', -1) as fn_whconsiid, fv_artsizecode,fn_qtykirim fn_qty
            FROM gis_db.t_returdtl a
            inner join gis_db.t_returmst b on a.fv_noretur=b.fv_noretur
            where fd_tgltransaksi between {fd_date}
            union all 
            SELECT a.fv_noretur fc_nodoc,SUBSTRING_INDEX(a.fv_noretur, '/', -1) as fn_whconsiid, fv_artsizecode,fn_qtykirim fn_qty
            FROM gis_db.t_returbzdtl a
            inner join gis_db.t_returbzmst b on a.fv_noretur=b.fv_noretur
            where fd_tgltransaksi between {fd_date}
        ) send
        LEFT JOIN (
            SELECT fc_nosj, fv_artsizecode, fn_qtyterima fROM gis_db.t_receivingmst a 
            inner join gis_db.t_receivingdtl b on a.fv_noreceiving=b.fv_noreceiving
            UNION ALL 
            SELECT fc_nosj, fv_artsizecode, fn_qtyterima fROM gis_db.t_receivingbzmst a 
            inner join gis_db.t_receivingbzdtl b on a.fv_noreceiving=b.fv_noreceiving
            UNION ALL 
            SELECT fc_nodocstore fc_nosj, fv_artsizecode,fn_qty fn_qtyterima FROM gis_db.returnmst_tb a
            inner join gis_db.returndtl_tb b on a.fc_nodoc=b.fc_nodoc
        ) e ON send.fc_nodoc = e.fc_nosj and send.fv_artsizecode = e.fv_artsizecode
        '''
    with get_engine('write') as engine:  # Menggunakan context manager dengan with
                # Mencari tanggal Opname terakhir per tglnow
        df_rm=pd.read_sql(query, engine)
    df_rm=df_rm.fillna(0)
    return df_rm

def df__date_arrival():
    query = f"""
            SELECT 
                * 
            FROM gis_local.date_arrival_do 
        """
    with get_engine('imp') as engine:  # Menggunakan context manager dengan with
        df_date_arv = pd.read_sql(query, engine)
    return df_date_arv

def parse_sql_params(colom="*", distinct=False, order_by="", limit=None, **filters):
    distinct_clause = "DISTINCT" if distinct else ""
    where_conditions = ["1=1"]

    for key, value in filters.items():
        if value is None:
            continue

        operator = "="
        clean_key = key

        # Parsing suffix operator
        if key.endswith("__is_not_null") or key.endswith("__not_null"):
            clean_key = key.replace("__is_not_null", "").replace("__not_null", "")
            operator = "IS NOT NULL"
        elif key.endswith("__isnull") or key.endswith("__is_null"):
            clean_key = key.replace("__isnull", "").replace("__is_null", "")
            operator = "IS NULL" if bool(value) else "IS NOT NULL"
        elif key.endswith("__between"):
            clean_key = key.replace("__between", "")
            operator = "BETWEEN"
        elif key.endswith("__not_in"):
            clean_key = key.replace("__not_in", "")
            operator = "NOT IN"
        elif key.endswith("__like"):
            clean_key = key.replace("__like", "")
            operator = "LIKE"
        elif key.endswith("__in"):
            clean_key = key.replace("__in", "")
            operator = "IN"
        elif key.endswith("__not") or key.endswith("__neq"):
            clean_key = key.replace("__not", "").replace("__neq", "")
            operator = "<>"
        elif key.endswith("__gt"):
            clean_key = key.replace("__gt", "")
            operator = ">"
        elif key.endswith("__gte"):
            clean_key = key.replace("__gte", "")
            operator = ">="
        elif key.endswith("__lt"):
            clean_key = key.replace("__lt", "")
            operator = "<"
        elif key.endswith("__lte"):
            clean_key = key.replace("__lte", "")
            operator = "<="

        # Replace double underscore '__' dengan titik '.'
        clean_key = clean_key.replace("__", ".")
        # 2. Format Value
        if operator in ["IS NULL", "IS NOT NULL"]:
            # Khusus NULL check, tidak perlu mencantumkan value angka/string di SQL
            where_conditions.append(f"{clean_key} {operator}")
        elif operator == "BETWEEN":
            if isinstance(value, (list, tuple, set)) and len(value) == 2:
                v1 = f"'{value[0]}'" if isinstance(value[0], str) else str(value[0])
                v2 = f"'{value[1]}'" if isinstance(value[1], str) else str(value[1])
                where_conditions.append(f"{clean_key} BETWEEN {v1} AND {v2}")
            else:
                # Abaikan jika format value untuk BETWEEN tidak valid
                continue
        elif isinstance(value, (list, tuple)):
            if not value:
                continue
            formatted_values = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in value])
            op_list = operator if operator in ["IN", "NOT IN"] else "IN"
            where_conditions.append(f"{clean_key} {op_list} ({formatted_values})")
        elif isinstance(value, str):
            where_conditions.append(f"{clean_key} {operator} '{value}'")
        else:
            where_conditions.append(f"{clean_key} {operator} {value}")

    # PASTI KAN ADA SPASI / KONSISTENSI KLAUSA
    where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    order_clause = f" ORDER BY {order_by}" if order_by else ""
    limit_clause = f" LIMIT {limit}" if limit else ""

    return distinct_clause, colom, where_clause, order_clause, limit_clause

def df__draft_pending(colom="*", distinct=False, 
                            order_by="", limit=None,df=True, queri=False, **filters):
    # 1. Ekstrak komponen SQL dari parameter
    distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
        colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters
    )
    # 2. Rakit Query
    query = f"""
        SELECT {distinct_cl} {cols}
        FROM t_draftdtl a
        left join t_draftmst b on a.fv_nodraft=b.fv_nodraft
        {where_cl}{order_cl}{limit_cl}
    """
    if queri:
        print("--- RUNNING QUERY ---")
        print(query)
    
    # 3. Eksekusi
    with get_engine('imp') as engine:  # Menggunakan context manager dengan with
                # Mencari tanggal Opname terakhir per tglnow
        results=pd.read_sql(query, engine)
    return results



