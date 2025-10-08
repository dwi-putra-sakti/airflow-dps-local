from modules.db_connection import get_engine, get_db
from datetime import datetime, date
import pandas as pd
import numpy as np

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
    SELECT * FROM gis_local.trxstx_pb25 
    where fv_stat like 'receive%%' and fd_tgltrx <= '{input_value}') a
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
    fv_fitname , fv_tomname , fv_configname , motif, fv_picture, fv_rating, fm_cogs, fn_articleid, fv_fitting

    from (
    select a.fn_articleid, c.fv_brandname, e.fv_divname, d.fv_colorname, g.fv_sizename, k.fv_tomname, 
    b.fv_artsizecode, a.fv_barcode, h. fv_catname, f.fv_namemark, a.fm_price, f.fm_amount, i.fv_configname, 
    l.motif, a.fv_picture, m.fv_configname fv_rating, a.fm_cogs,
    concat(h. fv_catname,' ',j.fv_fitname) as fv_fitname, b.fn_artsizeid, j.fv_fitname fv_fitting
    
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
    with get_engine('read') as engine:  # Menggunakan context manager dengan with
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
    with get_engine('read') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__markdown():
    query = f'''
        SELECT a.fn_whconsiid, f.fc_hold, f.fv_namecus AS Toko, f.fn_grpcusid,
            c.fv_provincename AS Provinsi, d.fv_cityname AS Kota, f.fn_cusid, f.fc_codecus AS code,
            e.fc_consinetto, e.fv_namegrpcus, h.fv_configname
        FROM warehouseconsi_tb a'''
def df__cus_user():
    query = f'''
        SELECT a.fn_userid, fn_cusid FROM 
    (select fn_userid from gis_db1.user_tb where fn_userlevel=2 and fc_hold='F') a 
    left join (SELECT fn_userid, fn_cusid FROM gis_db1.t_aksesusercust)b on a.fn_userid=b.fn_userid
    where fn_cusid is not null
        '''
    with get_engine('read') as engine:  # Menggunakan context manager dengan with
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
    with get_engine('read') as engine:  # Menggunakan context manager dengan with
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
    with get_engine('read') as engine:  # Menggunakan context manager dengan with
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
        SELECT {col} from gis_db1.articlesize_tb
        '''
        with get_engine('read') as engine:  # Menggunakan context manager dengan with
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
        SELECT fn_sizeid, fv_sizename FROM gis_db1.size_tb
        '''
    with get_engine('read') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query, engine)
    return df

def df__tokoawal():
    query = ''' 
    select c.fn_whconsiid from customer_tb a 
    inner join groupcustomer_tb b on a.fn_grpcusid = b.fn_grpcusid
    inner join warehouseconsi_tb c on a.fn_cusid=c.fn_cusid
    where b.fn_typecust='58' 
    '''  
    with get_engine('read') as engine:  # Menggunakan context manager dengan with
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
            WHERE a.fn_whconsiid = %s AND SUBSTR(a.fd_tglflag, 1, 10) <= %s
            ORDER BY a.fd_tglflag DESC LIMIT 1
            '''
    with get_engine('read') as engine:  # Menggunakan context manager dengan with
                # Mencari tanggal Opname terakhir per tglnow
        df = pd.read_sql(query, engine, params=(idwhtoko, tglnow))
    return df

def df__tglso_():
    """
    Fungsi untuk mendapatkan tanggal Opname terakhir berdasarkan idwhtoko dan tglnow.
    """
    query = '''
            SELECT DISTINCT a.fn_whconsiid, DATE_FORMAT(a.fd_tglflag, '%%Y-%%m-%%d') AS fd_tglflagIndo
            FROM fl_stockarticleconsipostingmst a
            '''
    with get_engine('read') as engine:  # Menggunakan context manager dengan with
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
    with get_engine('read') as engine:  # Menggunakan context manager dengan with
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
    with get_engine('read') as engine:  # Menggunakan context manager dengan with
                # Mencari tanggal Opname terakhir per tglnow
        df = pd.read_sql(query, engine)
    return df

def df__stock_so(idwhtoko, tglSTR, tglEND):
    query = '''
    select a.fv_artsizecode, a.fn_qtyopname qty
    from gis_db1.t_historysimpanstokopnamecounter a
    inner join gis_db1.fl_stockarticleconsipostingmst b on a.fc_nodoc=b.fc_nodoc
    where b.fn_whconsiid= '%s' AND SUBSTR(b.fd_tglflag, 1, 10) between %s AND %s
    GROUP BY a.fv_artsizecode
    '''
    with get_engine('read') as engine:  # Menggunakan context manager dengan with
                # Mencari tanggal Opname terakhir per tglnow
        df = pd.read_sql(query, engine, params=(idwhtoko, tglSTR, tglEND))
    return df

def df__stock_sa(idwhtoko, tglEND):
    query = '''
    select a.fv_artsizecode, a.fn_qty qty
    from saldoawalarticleconsi_tb a
    where a.fn_whconsiid= '%s' AND SUBSTR(fd_tglinject, 1, 10) <= %s
    '''
    with get_engine('read') as engine:  # Menggunakan context manager dengan with
                # Mencari tanggal Opname terakhir per tglnow
        df = pd.read_sql(query, engine, params=(idwhtoko, tglEND))
    return df

def df__transaction(idwhtoko, tglSTR, tglEND):
    queries = [
    ''' SELECT b.fv_artsizecode, IFNULL(SUM(b.fn_qtyterima), 0) AS qty
        FROM t_receivingmst a
        INNER JOIN t_receivingdtl b ON a.fv_noreceiving=b.fv_noreceiving
        WHERE a.fv_noreceiving LIKE %s AND a.fc_status<>'F'
        AND (a.fd_tglreceive BETWEEN %s AND %s)
        GROUP BY b.fv_artsizecode''',
    # AdjustIN
    ''' SELECT b.fv_artsizecode, IFNULL(SUM(b.fn_qty), 0) AS qty
        FROM t_adjustconsi_mst a
        INNER JOIN t_adjustconsi_dtl b ON a.fc_nodoc=b.fc_nodoc
        WHERE a.fn_whconsiid= '%s' AND a.fc_status='X' 
        AND b.fc_tipeadj='I' AND b.fn_useridbatal IS NULL
        AND (a.fd_tgladj BETWEEN %s AND %s)
        GROUP BY b.fv_artsizecode''',
    # Batal Penjualan
    ''' SELECT b.fv_artsizecode, IFNULL(SUM(b.fn_jualpersize), 0) AS qty
        FROM t_penjualanmst a 
        INNER JOIN t_penjualandtl_goods b ON a.fv_nopenjualan=b.fv_nopenjualan
        WHERE a.fn_cusid= '%s' AND (a.fd_tgljual BETWEEN %s AND %s) 
        AND b.fc_status='F'
        GROUP BY b.fv_artsizecode''',
    # Batal Retur
    ''' SELECT b.fv_artsizecode, IFNULL(SUM(b.fn_qtykirim), 0) AS qty
        FROM t_returmst a
        INNER JOIN t_returdtl b ON a.fv_noretur=b.fv_noretur
        WHERE a.fn_whconsiid='%s' AND (a.fd_tgltransaksi BETWEEN %s AND %s)
        AND b.fc_status='F'
        GROUP BY b.fv_artsizecode''',
    # Batal Mutasi
    ''' SELECT b.fv_artsizecode, IFNULL(SUM(b.fn_qtykirim), 0) AS qty
        FROM t_mutasimst a
        INNER JOIN t_mutasidtl b ON a.fv_nomutasi=b.fv_nomutasi
        WHERE a.fn_whconsiid='%s' AND (a.fd_tgltransaksi BETWEEN %s AND %s)
        AND b.fc_status='F'
        GROUP BY b.fv_artsizecode''',
    # Penjualan
    ''' SELECT a.fv_artsizecode, IFNULL(SUM(a.fn_jualpersize), 0) * -1 AS qty
        FROM t_penjualandtl_goods a
        INNER JOIN t_penjualanmst b ON a.fv_nopenjualan=b.fv_nopenjualan
        WHERE b.fn_cusid='%s' AND (b.fd_tgljual BETWEEN  %s AND %s)
        AND a.fv_nopenjualan LIKE 'PJ%%'
        GROUP BY a.fv_artsizecode''',
    # AdjustOUT 
    ''' SELECT b.fv_artsizecode, IFNULL(SUM(b.fn_qty), 0) * -1 AS qty
        FROM t_adjustconsi_mst a
        INNER JOIN t_adjustconsi_dtl b ON a.fc_nodoc=b.fc_nodoc
        WHERE a.fn_whconsiid='%s' AND (a.fd_tgladj BETWEEN %s AND %s)
        AND a.fc_status='X' AND b.fc_tipeadj='O' AND b.fn_useridbatal IS NULL
        GROUP BY b.fv_artsizecode''',
    # Retur 
    ''' SELECT a.fv_artsizecode, IFNULL(SUM(a.fn_qtykirim), 0) * -1 AS qty
        FROM t_returdtl a
        INNER JOIN t_returmst b ON a.fv_noretur=b.fv_noretur
        WHERE b.fn_whconsiid='%s' AND a.fv_noretur LIKE 'RT%%'
        AND (b.fd_tgltransaksi BETWEEN %s AND %s)
        GROUP BY a.fv_artsizecode''',
    # Mutasi
    ''' SELECT a.fv_artsizecode, IFNULL(SUM(a.fn_qtykirim), 0) * -1 AS qty
        FROM t_mutasidtl a
        INNER JOIN t_mutasimst b ON a.fv_nomutasi=b.fv_nomutasi
        WHERE b.fn_whconsiid='%s' AND a.fv_nomutasi LIKE 'MT%%'
        AND (b.fd_tgltransaksi BETWEEN %s AND %s)
        GROUP BY a.fv_artsizecode'''
    # Tambahkan query lainnya...
    ]
    with get_engine('read') as engine_read:
    # Membuat DataFrame gabungan
        # DataFrame untuk menyimpan hasil
        combined = pd.DataFrame()

        for query in queries:
            # Menentukan parameter query
            if "t_receivingdtl" in query:
                params = (f"%/{idwhtoko}", tglSTR, tglEND)
            else:
                params = (idwhtoko, tglSTR, tglEND)

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
    with get_engine('read') as engine:
        """Mengambil data penjualan per bulan dari database."""
        months = generate_months_for_year(start_year, end_year)
        sales_data = []
        for month, period in months.items():
            start_date, end_date = period.split(" to ")
            
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
            '''.format(month=f"{month}")
            
            # Menggunakan parameter untuk mengganti nilai tanggal
            df = pd.read_sql_query(query, engine, params=(start_date, end_date))
            sales_data.append(df)
    combined_sales = pd.concat(sales_data, axis=0, ignore_index=True)
    combined_sales = combined_sales.groupby(['fn_whconsiid', 'fv_artsizecode'], as_index=False).sum()
    return combined_sales

def df__retur(start_year):
    query_toko_awal = f''' 
        SELECT fn_whconsiid, fv_artsizecode, sum(qty) qty FROM trx_retur_pb 
        where tgltrx like '%%{start_year}' GROUP BY fn_whconsiid, fv_artsizecode
        '''
    with get_engine('read') as engine_read:
        df = pd.read_sql(query_toko_awal, engine_read)
    return df

def df__sales_thru_24(col):
    query = f'''
        SELECT {col} FROM gis_local.sales_thru_pb_24
        '''
    with get_engine('imp') as engine_imp:
        df = pd.read_sql(query, engine_imp)
    return df

def df__sales_thru_25(col):
    query = f'''
        SELECT {col} FROM gis_local.sales_thru_pb_25
        '''
    with get_engine('imp') as engine_imp:
        df = pd.read_sql(query, engine_imp)
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
    where fv_whtypename in ('RETUR','PUSAT','KONSINETTO','QC') and Artikel is not null
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
    with get_engine('read') as engine:  # Menggunakan context manager dengan with
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
        ) AS subquery
    GROUP BY fn_whconsiid, fv_artsizecode, tgltrx, no_doc;
        '''
        df= pd.read_sql(query, engine)
        print(today_str)
        print(query)
    return df
    
def fetch_data_margin(year):
    with get_engine('imp') as engine:
    # Fetch data for the given year        
        query_artimp = '''
        SELECT fv_barcode, fv_artsizecode 
        FROM gis_local.artikel_pb
        '''
        df_artimp = pd.read_sql(query_artimp, engine)
    with get_engine('read') as engine:
        query_trx = f'''
        SELECT fn_idurut, fn_whconsiid, fd_tgljual fd_tgltrx, fv_artsizecode 
        FROM gis_db1.t_penjualandtl_goods where fd_tgljual BETWEEN '{year}-01-01' AND '{year}-12-31'
        '''
        df_trx = pd.read_sql(query_trx, engine)
        
        query_artred = '''
        SELECT fn_catid, fv_barcode 
        FROM gis_db1.article_tb
        '''
        df_artred = pd.read_sql(query_artred, engine)
        
        query_atrxu = f'''
        SELECT fn_idurut, markid2, fn_markid 
        FROM (
            SELECT fn_idurut, fn_markid markid2, fn_idevent 
            FROM gis_db1.t_penjualandtl_goods 
            WHERE fd_tgljual BETWEEN '{year}-01-01' AND '{year}-12-31'
        ) a 
        LEFT JOIN (SELECT fn_idevent, fn_discid FROM gis_db1.event_tb) b ON a.fn_idevent = b.fn_idevent
        LEFT JOIN (SELECT fn_discid, fn_markid FROM gis_db1.discount_tb) c ON b.fn_discid = c.fn_discid
        '''
        df_atrxu = pd.read_sql(query_atrxu, engine)
        
        query_mrgincat = f'''
        SELECT fn_whconsiid, fn_catid, fn_markid, fd_begindate, fd_enddate, fn_margin 
        FROM (
            SELECT * 
            FROM gis_db1.margincat_tb 
            WHERE fd_begindate > '{year-1}-12-31 00:00:00' AND fd_enddate < '{year+1}-01-01 00:00:00'
        ) a
        LEFT JOIN (SELECT fc_code, fn_cusid FROM gis_db1.margincatstore_tb) b ON a.fc_code = b.fc_code
        LEFT JOIN (SELECT fn_cusid, fn_whconsiid FROM gis_db1.warehouseconsi_tb) c ON b.fn_cusid = c.fn_cusid
        '''
        df_mrgincat = pd.read_sql(query_mrgincat, engine)
        
        query_mrgin = f'''
        SELECT fn_whconsiid, fn_markid, fd_effectivedate, fn_margin 
        FROM (
            SELECT fn_marginid, fd_effectivedate, fn_markid, fn_margin 
            FROM gis_db1.margin_tb 
            WHERE fd_effectivedate BETWEEN '{year}-01-01' AND '{year}-12-31'
        ) a
        LEFT JOIN (SELECT fn_marginid, fn_cusid FROM gis_db1.margindtl_tb) b ON a.fn_marginid = b.fn_marginid
        LEFT JOIN (SELECT fn_cusid, fn_whconsiid FROM gis_db1.warehouseconsi_tb) c ON b.fn_cusid = c.fn_cusid
        '''
        df_mrgin = pd.read_sql(query_mrgin, engine)
        
    return df_trx, df_artimp, df_artred, df_atrxu, df_mrgincat, df_mrgin

def df__sls_putus_disc():
    with get_engine('read') as engine:
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
    with get_engine('read') as engine:
        query='''
        SELECT a.fc_nodoc, a.fn_cusid, a.fd_invdate tgltrx, b.fv_artsizecode, fn_qty, fm_netto hargajual, fm_dpp/fn_qty dpp  FROM salesinvdtlnew_tb b
        inner join salesinvmstnew_tb a on a.fc_nodoc=b.fc_nodoc
        '''
        df_si= pd.read_sql(query, engine)
    return df_si

def df__sales_bz():
    with get_engine('read') as engine:
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
    with get_engine('read') as engine:
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
    with get_engine('read') as engine:
        query='''select a.fv_nopenjualan, fn_potonganharga, fn_totalqty qty from
        (SELECT fv_nopenjualan, fn_potonganharga FROM t_penjualanmst
        where fn_potonganharga <> 0) a 
        left join (SELECT fv_nopenjualan, sum(fn_jualpersize) fn_totalqty FROM t_penjualandtl_goods
        group by fv_nopenjualan) b on a.fv_nopenjualan=b.fv_nopenjualan
        '''
        df= pd.read_sql(query, engine)
    return df

def df__sales_online_disc2nd():
    with get_engine('read') as engine:
        query='''select a.fv_nopenjualan, servisfee, premi, affiliate, voucher, fn_totalqty qty from
        (SELECT fv_nopenjualan, fm_asuransi servisfee, fm_ongkir premi, fm_voucher affiliate, fm_lainlain voucher FROM gis_db1.t_penjualanpotongan
        ) a 
        left join (SELECT fv_nopenjualan, sum(fn_jualpersize) fn_totalqty FROM t_penjualandtl_goods
        group by fv_nopenjualan) b on a.fv_nopenjualan=b.fv_nopenjualan
        '''
        df= pd.read_sql(query, engine)
    return df

def df__otsdo():
    today = date.today()
    year = today.year
    with get_engine('read') as engine:
        query= f'''SELECT 
        a.fc_nodoc ,fv_artsizecode, sum(fn_qty) fn_qty, fd_date, b.fc_status, fn_whconsiid, b.fd_dateinput
        FROM gis_db1.dodtl_tb a 
        inner join gis_db1.domst_tb b on a.fc_nodoc=b.fc_nodoc
        inner join gis_db1.t_ordermst c on b.fc_noreff=fv_noorder
        left join gis_db1.t_receivingmst d on b.fc_nodoc=d.fc_nosj
        left join gis_db1.warehouseconsi_tb e on c.fn_cusid=e.fn_cusid
        where  fv_noreceiving is null and fd_date > '{year}-01-01 00:00:00'and b.fc_status="F" 
        group by fn_whconsiid, fv_artsizecode, fd_date 
        '''
        df= pd.read_sql(query, engine)
    return df
def df__otsdo():
    today = date.today()
    year = today.year
    with get_engine('read') as engine:
        query= f'''SELECT 
        a.fc_nodoc ,fv_artsizecode, sum(fn_qty) fn_qty, fd_date, b.fc_status, fn_whconsiid, b.fd_dateinput
        FROM gis_db1.dodtl_tb a 
        inner join gis_db1.domst_tb b on a.fc_nodoc=b.fc_nodoc
        inner join gis_db1.t_ordermst c on b.fc_noreff=fv_noorder
        left join gis_db1.t_receivingmst d on b.fc_nodoc=d.fc_nosj
        left join gis_db1.warehouseconsi_tb e on c.fn_cusid=e.fn_cusid
        where  fv_noreceiving is null and fd_date > '{year}-01-01 00:00:00'and b.fc_status="F" 
        group by fn_whconsiid, fv_artsizecode, fd_date 
        '''
        df= pd.read_sql(query, engine)
    return df

def df__otsdo_rt(tglinput):
    with get_engine('write') as engine:
        query= f'''SELECT 
        a.fc_nodoc ,fv_artsizecode, sum(fn_qty) fn_qty, fd_date, b.fc_status, c.fn_cusid, b.fd_dateinput
        FROM gis_db.dodtl_tb a 
        inner join gis_db.domst_tb b on a.fc_nodoc=b.fc_nodoc
        inner join gis_db.t_ordermst c on b.fc_noreff=fv_noorder
        left join gis_db.t_receivingmst d on b.fc_nodoc=d.fc_nosj
        where  fv_noreceiving is null and b.fd_dateinput > '{tglinput}' and b.fc_status="F" 
        group by fc_nodoc, fn_cusid, fv_artsizecode, fd_date 
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
    with get_engine('read') as engine:
        query= f'''SELECT 
            a.fc_nodoc ,fv_artsizecode, sum(fn_qty) fn_qty, fd_date, b.fc_status, fn_whconsiid, fn_whconsiid2
            FROM gis_db1.dodtl_tb a 
            inner join gis_db1.domst_tb b on a.fc_nodoc=b.fc_nodoc
            inner join gis_db1.t_mutasimst c on b.fc_noreff=c.fv_nomutasi
            left join gis_db1.t_receivingmst d on b.fc_nodoc=d.fc_nosj
            where  fv_noreceiving is null and fd_date > '{year}-01-01 00:00:00'
            and b.fc_status="F" 
            group by fn_whconsiid, fv_artsizecode, fd_date  
        '''
        df= pd.read_sql(query, engine)
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
    SELECT a.fc_nodoc, fd_arrivaldate, fv_nopo, fn_articleid, fn_qty FROM gis_db1.grdtl_tb a
    inner join gis_db1.grmst_tb b on a.fc_nodoc = b.fc_nodoc
    '''
    with get_engine('read') as engine:  # Menggunakan context manager dengan with
        df = pd.read_sql(query_gr, engine)
    return df
