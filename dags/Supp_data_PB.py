# Sup_data_PB
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from datetime import date
import calendar
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy import Integer, String, CHAR, Date, Text

# Informasi koneksi

host2 = "36.93.148.94"
port2 = 3366
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

def define_sql_types(df,tipe):

    """
    Menentukan tipe data SQL sesuai DataFrame dan menambahkan prefix pada kolom berdasarkan tipe data.
    """
    sql_types = {}
    renamed_columns = {}
    
    for col in df.columns:
        if pd.api.types.is_integer_dtype(df[col]):
            new_col_name = f'fn_{col}' if not col.startswith('fn_') else col
            sql_types[new_col_name] = Integer
        elif pd.api.types.is_string_dtype(df[col]):
            max_length = df[col].astype(str).str.len().max()
            if max_length > 255:
                new_col_name = f'fv_{col}' if not col.startswith('fv_') else col
                sql_types[new_col_name] = Text
            elif max_length <= 255:
                new_col_name = f'fv_{col}' if not col.startswith('fv_') else col
                sql_types[new_col_name] = String(max(50, min(max_length, 255)))
        elif col.startswith('fc'):
            new_col_name = f'fc_{col}' if not col.startswith('fc_') else col
            sql_types[new_col_name] = String(50)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            new_col_name = f'fd_{col}' if not col.startswith('fd_') else col
            sql_types[new_col_name] = Date
        else:
            new_col_name = f'fv_{col}' if not col.startswith('fv_') else col
            sql_types[new_col_name] = String(50)
        
        # Simpan mapping nama kolom baru
        renamed_columns[col] = new_col_name
    
    # Perbarui nama kolom di DataFrame
    df.rename(columns=renamed_columns, inplace=True)
    if tipe == 'yes':
        return sql_types

    return df

def save_to_database(df, table_name, engine):
    """Menyimpan DataFrame ke database."""
    sql_types = define_sql_types(df,'yes')
    df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=sql_types)

def data_aging():
    timestart = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("[INFO] Script data_aging dimulai.")
    print(f"[INFO] Timestart: {timestart}\n")
    with get_engine('read') as engine:
        query='''
        SELECT * FROM gis_db1.pb_tranx_receiving
        '''
        df_ag= pd.read_sql(query, engine)
    with get_engine('imp') as engine:  
        query='''
        SELECT fv_barcode, fv_artsizecode FROM artikel_pb
        '''
        df_artikel= pd.read_sql(query, engine)

    df_ag["tgltrx"] = pd.to_datetime(df_ag["tgltrx"])
    # Tanggal hari ini
    today = pd.Timestamp(datetime.now().date())
    df_ag["hari_di_toko"] = (today - df_ag["tgltrx"]).dt.days
    df_ag["bulan_di_toko"] = (today.year - df_ag["tgltrx"].dt.year) * 12 + (today.month - df_ag["tgltrx"].dt.month)
    df_ag2=pd.merge(df_ag,df_artikel,on='fv_artsizecode',how='left')
    result = df_ag2.groupby(["fn_whconsiid", "fv_barcode"]).agg(
    aging_days_min=("hari_di_toko", "min"),
    aging_days_max=("hari_di_toko", "max"),
    aging_months_min=("bulan_di_toko", "min"),
    aging_months_max=("bulan_di_toko", "max")
    ).reset_index()
    with get_engine('imp') as engine:
        result.to_sql('aging_pb', engine, if_exists='replace', index=False)

def data_sales_h():
    with get_engine('imp') as engine_imp:
        query='''
        SELECT fn_whconsiid, fv_artsizecode, fn_stock_at, fn_total_sales_qty FROM gis_local.sales_thru_pb
        '''
        df_sq= pd.read_sql(query, engine_imp)
        query='''
        SELECT Article,PROJECT,CATEGORY,BRAND,DIVISION FROM gis_local.artikel_md
        '''
        df_artmd= pd.read_sql(query, engine_imp)
        query='''
        SELECT fn_whconsiid, fv_toko, fc_codecus, fv_alokator FROM gis_local.customers_pb
        '''
        df_cus= pd.read_sql(query, engine_imp)
        query='''
        SELECT * FROM gis_local.aging_pb
        '''
        df_agi= pd.read_sql(query, engine_imp)

    with get_engine('read') as engine:
        query='''
            SELECT artikel, barcode, ukuran, jenis,
            namaartikel, brand, divisi, rating, acara, hargabarang FROM gis_db1.artikel_pb
            '''
        df_artsq= pd.read_sql(query, engine)
    df_sq2=pd.merge(df_sq,df_artsq,left_on='fv_artsizecode', right_on='barcode',how='left')
    # Pivot data untuk mendapatkan jumlah stok per ukuran
    pivot = df_sq2.pivot_table(
    index=["fn_whconsiid", "artikel"],  # Grup berdasarkan kolom ini
    columns="ukuran",                  # Kolom ukuran menjadi header
    values="fn_stock_at",              # Ambil nilai stok untuk tabel
    aggfunc="sum",                     # Agregasi (sum) jika ada data duplikat
    fill_value=0                       # Isi 0 jika tidak ada data
    )
    # Tambahkan kolom Grand Total
    pivot["stock"] = pivot.sum(axis=1)

    # Tambahkan kolom fn_total_sales_qty dari data asli
    sales_qty = df_sq2.groupby(["fn_whconsiid", "artikel"])["fn_total_sales_qty"].sum()
    pivot["sales_qty"] = sales_qty
    result = pivot.reset_index()

    df_arttpbt=df_artsq[['artikel','jenis']]
    df_sq3=pd.merge(result,df_arttpbt,on='artikel',how='left')
    # Definisikan kolom untuk masing-masing kategori
    top_columns = ["FS", "L", "M", "S", "XL", "XS", "XXL", "XXXL"]
    bottom_columns = ["25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "42", "44"]
    df=df_sq3
    # Terapkan logika menggunakan np.where
    df["status"] = np.where(
        (df["jenis"] == "TOP") & (df[top_columns].gt(0).sum(axis=1) < 2), "BROKEN", 
        np.where(
            (df["jenis"] == "BOTTOM") & (df[bottom_columns].gt(0).sum(axis=1) < 5), "BROKEN","SEHAT"
        ))

    df=df.drop_duplicates()
    df = df[(df['stock'] != 0) | (df['sales_qty'] != 0)]
    finale =define_sql_types(df,'no')
    df_detail1=df_artsq[['artikel','namaartikel','brand','divisi','rating','acara','hargabarang']]
    df_detail1=df_detail1.drop_duplicates()
    finale1 = pd.merge(df,df_artmd,left_on='fv_artikel',right_on='Article',how='left')
    finale1=finale1.drop_duplicates()

    finale2 = pd.merge(finale1,df_detail1,left_on='fv_artikel',right_on='artikel',how='left')
    finale2['CATEGORY'] = finale2['CATEGORY'].fillna(finale2['namaartikel'])
    finale2['BRAND'] = finale2['BRAND'].fillna(finale2['brand'])
    finale2['DIVISION'] = finale2['DIVISION'].fillna(finale2['divisi'])     
    finale2 = finale2.drop(columns= ['Article','artikel','namaartikel','brand','divisi'])

    finale3=pd.merge(finale2,df_cus,on='fn_whconsiid',how='left')
    df_agi["fn_whconsiid"] = df_agi["fn_whconsiid"].astype("int64")

    finale4=pd.merge(finale3,df_agi,left_on=['fn_whconsiid','fv_artikel'],right_on=['fn_whconsiid','artikel'],how='left')
    finale4 = finale4.drop(columns= ['artikel'])
    finale5= define_sql_types(finale4,'no')
    finale5=finale5.rename(columns={'fv_fc_codecus':'fv_codecus'})

    with get_engine('imp') as engine_imp:
        save_to_database(finale5,'stock_sales_h',engine_imp)
    
def calculate_duration(start, end):
    """Menghitung durasi antara dua waktu."""
    duration = (end - start).total_seconds() / 60
    return f"{int(duration)} menit" if duration >= 1 else f"{int(duration * 60)} detik"

def trxstx_pb(year):
    with get_engine('read') as engine:
        query=f'''
        SELECT fn_whconsiid, fv_artsizecode, IFNULL(SUM(totalplus), 0) AS stock, tgltrx, stat
    FROM (
    select
        SUBSTRING_INDEX(a.fv_noreceiving, '/', -1) AS fn_whconsiid,
        b.fv_artsizecode, 
        b.fn_qtyterima AS totalplus,
        DATE_FORMAT(a.fd_tglreceive, '%%m-%%d-%%Y') tgltrx, 'recieve' as stat
        FROM     t_receivingmst a
        INNER JOIN t_receivingdtl b ON a.fv_noreceiving = b.fv_noreceiving
        WHERE a.fc_status <> 'F'
        AND (a.fd_tglreceive BETWEEN '{year}-01-01 00:00:00' and '{year}-12-31 23:59:00')
        union all
    SELECT 
        a.fn_whconsiid,
        b.fv_artsizecode, 
        b.fn_qty as totalplus,
        DATE_FORMAT(a.fd_tgladj, '%%m-%%d-%%Y') tgltrx, 'adj_in' as stat
        FROM 
        t_adjustconsi_mst a 
        INNER JOIN 
        t_adjustconsi_dtl b ON a.fc_nodoc = b.fc_nodoc
        WHERE 
        a.fc_status = 'X' 
        AND b.fc_tipeadj = 'I' 
        AND b.fn_useridbatal IS NULL
        and (a.fd_tgladj between '{year}-01-01 00:00:00' and '{year}-12-31 23:59:00')
        union all
    SELECT 
        a.fn_cusid fn_whconsiid,
        b.fv_artsizecode, 
        b.fn_jualpersize as totalplus,
        DATE_FORMAT(a.fd_tgljual, '%%m-%%d-%%Y') tgltrx, 'b_penjualan' as stat
        FROM 
        t_penjualanmst a 
        INNER JOIN 
        t_penjualandtl_goods b ON a.fv_nopenjualan = b.fv_nopenjualan
        WHERE 
        b.fc_status = 'F'
        and (a.fd_tgljual between '{year}-01-01 00:00:00' and '{year}-12-31 23:59:00')
        union all
    SELECT 
        a.fn_whconsiid,
        b.fv_artsizecode, 
        b.fn_qtykirim as totalplus,
        DATE_FORMAT(a.fd_tgltransaksi, '%%m-%%d-%%Y') tgltrx, 'b_retur' as stat
        FROM 
        t_returmst a 
        INNER JOIN 
        t_returdtl b ON a.fv_noretur=b.fv_noretur
        WHERE 
        b.fc_status='F'
        AND (a.fd_tgltransaksi BETWEEN '{year}-01-01 00:00:00' and '{year}-12-31 23:59:00')
        union all
    SELECT 
        a.fn_whconsiid,
        b.fv_artsizecode, 
        b.fn_qtykirim as totalplus,
        DATE_FORMAT(a.fd_tgltransaksi, '%%m-%%d-%%Y') tgltrx, 'b_mutasi' as stat
        FROM 
        t_mutasimst a 
        INNER JOIN 
        t_mutasidtl b ON a.fv_nomutasi=b.fv_nomutasi
        WHERE 
        b.fc_status='F'
        AND (a.fd_tgltransaksi BETWEEN '{year}-01-01 00:00:00' and '{year}-12-31 23:59:00')
        union all
    SELECT 
        fn_cusid fn_whconsiid,
        b.fv_artsizecode, 
        -1 * b.fn_jualpersize as totalplus,
        DATE_FORMAT(a.fd_tgljual, '%%m-%%d-%%Y') tgltrx, 'penjualan' as stat
        FROM 
        t_penjualanmst a 
        INNER JOIN 
        t_penjualandtl_goods b ON a.fv_nopenjualan = b.fv_nopenjualan
        WHERE 
        a.fv_nopenjualan LIKE 'PJ%%'
        AND (b.fd_tgljual BETWEEN  '{year}-01-01 00:00:00' and '{year}-12-31 23:59:00')
        union all
    SELECT 
        fn_whconsiid,
        b.fv_artsizecode, 
        -1 * b.fn_qty as totalplus,
        DATE_FORMAT(a.fd_tgladj, '%%m-%%d-%%Y') tgltrx, 'adj_out' as stat
        FROM 
        t_adjustconsi_mst a 
        INNER JOIN 
        t_adjustconsi_dtl b ON a.fc_nodoc = b.fc_nodoc
        WHERE 
        a.fc_status = 'X' 
        AND b.fc_tipeadj = 'O' 
        AND b.fn_useridbatal IS NULL
        AND (a.fd_tgladj BETWEEN '{year}-01-01 00:00:00' and '{year}-12-31 23:59:00')
        union all
    SELECT 
        a.fn_whconsiid,
        b.fv_artsizecode, 
        -1 * b.fn_qtykirim as totalplus,
        DATE_FORMAT(a.fd_tgltransaksi, '%%m-%%d-%%Y') tgltrx, 'retur' as stat
        FROM 
        t_returmst a 
        INNER JOIN 
        t_returdtl b ON a.fv_noretur=b.fv_noretur
        WHERE 
        a.fv_noretur LIKE 'RT%%' 
        AND (a.fd_tgltransaksi BETWEEN '{year}-01-01 00:00:00' and '{year}-12-31 23:59:00')
        union all
    SELECT 
        a.fn_whconsiid,
        b.fv_artsizecode, 
        -1 * b.fn_qtykirim as totalplus,
        DATE_FORMAT(a.fd_tgltransaksi, '%%m-%%d-%%Y') tgltrx, 'mutasi' as stat
        FROM 
        t_mutasimst a 
        inner JOIN 
        t_mutasidtl b ON a.fv_nomutasi=b.fv_nomutasi
        WHERE 
        a.fv_nomutasi LIKE 'MT%%'
        AND (a.fd_tgltransaksi BETWEEN '{year}-01-01 00:00:00' and '{year}-12-31 23:59:00')
        ) AS subquery
    GROUP BY fn_whconsiid, fv_artsizecode, tgltrx;
        '''
        df_trxstx= pd.read_sql(query, engine)
    df_trxstx['tgltrx'] = pd.to_datetime(df_trxstx['tgltrx'], format='%m-%d-%Y')
    df_trxstx['fn_whconsiid']=df_trxstx['fn_whconsiid'].astype(int)
    df_trxstx['stock']=df_trxstx['stock'].astype(int)
    df_trxstx=df_trxstx.rename(columns={
        'stock':'fn_stock', 
        'tgltrx':'fd_tgltrx', 
        'stat':'fv_stat'
    })

    with get_engine('imp') as engine:
            sql_types = define_sql_types(df_trxstx,'yes')
            df_trxstx.to_sql(f'trxstx_pb{str(year)[-2:]}', engine, if_exists='replace', index=False, dtype=sql_types)
    with get_engine('read') as engine:
            sql_types = define_sql_types(df_trxstx,'yes')
            df_trxstx.to_sql(f'trxstx_pb{str(year)[-2:]}', engine, if_exists='replace', index=False, dtype=sql_types)


def stock_gudang_pb():
    with get_engine('read') as engine:
        query='''select 
        fv_barcode Artikel,
        fv_artsizecode ,
        fv_whname ,
        fv_whtypename ,
        fn_qty stock,
        fn_booked booked

        From (
        Select a.fv_artsizecode, b.fv_whname, c.fv_whtypename, a.fn_qty, a.fn_booked, e.fv_barcode
        from stockarticle_tb a
        left join (SELECT * from warehouse_tb where fc_hold = "F") b on a.fn_whid=b.fn_whid
        left join (SELECT * from warehousetype_tb where fc_hold = "F") c on a.fn_whtypeid=c.fn_whtypeid
        left join articlesize_tb d on a.fv_artsizecode=d.fv_artsizecode
        left JOIN (select * from article_tb where fc_hold='F') e ON d.fn_articleid=e.fn_articleid
        ) as sub'''
        df= pd.read_sql(query, engine)
    with get_engine('imp') as engine:
            df.to_sql('stock_gudang_pb', engine, if_exists='replace', index=False)
    return

def Stock_varian ():
    with get_engine('read') as engine:
        query=''' 
        select fn_whconsiid,  fv_artsizecode, stock, DATE_FORMAT(tgl, '%%m-%%d-%%Y') tgl, stat
        from (
        select fn_whconsiid, fv_artsizecode, fn_varian stock, fd_tanggal tgl, "varian" as stat
        from  t_historysimpanstokopnamecounter
        where fn_varian <> 0 and fd_tanggal > '2023-1-1'

        union all

        SELECT a.fn_whconsiid, a.fv_artsizecode, (a.fn_qtyopname * -1) AS stock, a.fd_tanggal AS tgl, "tutup" as stat
        FROM t_historysimpanstokopnamecounter a
        INNER JOIN warehouseconsi_tb c ON a.fn_whconsiid = c.fn_whconsiid
        INNER JOIN customer_tb b ON b.fn_cusid = c.fn_cusid
        WHERE b.fc_hold = 'T' AND a.fn_qtyopname <> 0 AND a.fd_tanggal > '2024-01-01'
        AND a.fd_tanggal = (SELECT MAX(a2.fd_tanggal) FROM t_historysimpanstokopnamecounter a2 WHERE a2.fn_whconsiid = a.fn_whconsiid)

        union all 

        select a.fn_whconsiid, fv_artsizecode, fn_qty stock, fd_tglinject tgl, "awal" as stat
        from saldoawalarticleconsi_tb a
        inner join warehouseconsi_tb c on a.fn_whconsiid=c.fn_whconsiid
        inner join customer_tb b on b.fn_cusid=c.fn_cusid
        inner join groupcustomer_tb d on b.fn_grpcusid = d.fn_grpcusid
        where fd_tglinject > '2024-1-1' and d.fn_typecust='58' 
        ) as VAT'''
        df= pd.read_sql(query, engine)
    with get_engine('imp') as engine:
        df.to_sql('Stock_varian', engine, if_exists='replace', index=False)
    return

def trx_gudang24 ():
    with get_engine('read') as engine: 
        query='''SELECT fv_whname, fv_whtypename, fv_artsizecode, qty, DATE_FORMAT(tgl, '%%m/%%d/%%Y') tgl, Stat
		from(
		-- Stock GR
		select x.fv_whname, y.fv_whtypename, fv_artsizecode, fn_qty qty, fd_arrivaldate tgl, 'Stock GR' as 'Stat'
		from (select a.fc_nodoc, a.fn_whid, a.fd_arrivaldate, b.fn_whtypeid 
			from grmst_tb a 
			inner join warehousetypedtl_tb b on a.fn_rtid=b.fn_rtid and a.fn_rackid=b.fn_rackid) a
		inner join warehouse_tb x on a.fn_whid=x.fn_whid
		inner join warehousetype_tb y on a.fn_whtypeid=y.fn_whtypeid
		inner join grdtl_tb b on a.fc_nodoc=b.fc_nodoc
		where fd_arrivaldate > '2024-1-1'

		union all 

		-- Stock In Gudang
		select x.fv_whname, y.fv_whtypename, fv_artsizecode, fn_qty qty, a.fd_dateinput tgl, 'Stock In Gudang' as 'Stat'
		from (select a.fc_nodoc, a.fn_whid, a.fd_dateinput, b.fn_whtypeid 
			from stockinmst_tb a 
			inner join warehousetypedtl_tb b on a.fn_rtid=b.fn_rtid and a.fn_rackid=b.fn_rackid) a
		inner join warehouse_tb x on a.fn_whid=x.fn_whid
		inner join warehousetype_tb y on a.fn_whtypeid=y.fn_whtypeid
		inner join stockindtl_tb b on a.fc_nodoc=b.fc_nodoc
		where a.fd_dateinput > '2024-1-1'

		union all 

		-- Stock Out Gudang
		select x.fv_whname, y.fv_whtypename, fv_artsizecode, (fn_qty*-1) qty, a.fd_dateinput tgl, 'Stock Out Gudang' as 'Stat'
		from (select a.fc_nodoc, a.fn_whid, a.fd_dateinput, b.fn_whtypeid 
			from stockoutmst_tb a 
			inner join warehousetypedtl_tb b on a.fn_rtid=b.fn_rtid and a.fn_rackid=b.fn_rackid) a
		inner join warehouse_tb x on a.fn_whid=x.fn_whid
		inner join warehousetype_tb y on a.fn_whtypeid=y.fn_whtypeid
		inner join stockoutdtl_tb b on a.fc_nodoc=b.fc_nodoc
		where a.fd_dateinput > '2024-1-1'

		union all 

		-- Stock DO
		select x.fv_whname, y.fv_whtypename, fv_artsizecode, (fn_qty*-1) qty, a.fd_date tgl, 'Stock DO' as 'Stat'
		from (select a.fc_nodoc, b.fn_whid, a.fd_date
			from domst_tb a 
			inner join (select distinct fn_whid, fv_noorder from t_orderdtl_goods_tmp ) b on a.fc_noreff=b.fv_noorder) a
		inner join warehousetype_tb y on a.fn_whid=y.fn_whtypeid
		inner join warehouse_tb x on y.fn_whid=x.fn_whid
		inner join dodtl_tb b on a.fc_nodoc=b.fc_nodoc
		where a.fd_date > '2024-1-1' and a.fc_nodoc like 'DO/%%' and fn_qty <> 0
		) SG '''
        df= pd.read_sql(query, engine)
    with get_engine('imp') as engine:
        df.to_sql('trx_gudang24', engine, if_exists='replace', index=False)
    return

def trx_retur ():
    with get_engine('read') as engine:
        query='''
	SELECT 
		a.fn_whconsiid,
		b.fv_artsizecode, 
		b.fn_qtykirim as qty,
		DATE_FORMAT(a.fd_tgltransaksi, '%%m-%%d-%%Y') tgltrx
		FROM 
		t_returmst a 
		INNER JOIN 
		t_returdtl b ON a.fv_noretur=b.fv_noretur
		WHERE 
		b.fc_status='F'
		AND (a.fd_tgltransaksi BETWEEN '2023-12-31 23:59:59' and DATE(NOW()))
	union all
		SELECT 
		a.fn_whconsiid,
		b.fv_artsizecode, 
		-1 * b.fn_qtykirim as qty,
		DATE_FORMAT(a.fd_tgltransaksi, '%%m-%%d-%%Y') tgltrx
		FROM 
		t_returmst a 
		INNER JOIN 
		t_returdtl b ON a.fv_noretur=b.fv_noretur
		WHERE 
		a.fv_noretur LIKE 'RT%%' 
		AND (a.fd_tgltransaksi BETWEEN '2023-12-31 23:59:59' and DATE(NOW()))'''
        df= pd.read_sql(query, engine)
    with get_engine('imp') as engine:
        df.to_sql('trx_retur', engine, if_exists='replace', index=False)
    return

def Stock_Awal_Gudang24 ():
    with get_engine('read') as engine:
        query='''
		select 
		x.fv_whname, y.fv_whtypename, fv_artsizecode, fn_qtyold qty, b.fn_qtyin qty2, DATE_FORMAT(a.fd_postingdate, '%%m/%%d/%%Y') tgl
		from stockarticlepostingmst_tb a 
		inner join stockarticlepostingdtl_tb b on a.fc_nodoc=b.fc_nodoc
		inner join warehouse_tb x on b.fn_whid=x.fn_whid
		inner join warehousetype_tb y on b.fn_whtypeid=y.fn_whtypeid
		where fd_postingdate > '2024-1-1' and fn_qtyold <> 0
		order by fd_postingdate desc'''
        df= pd.read_sql(query, engine)
    with get_engine('imp') as engine:
        df.to_sql('Stock_Awal_Gudang24', engine, if_exists='replace', index=False)
    return

def retur_gd_pb ():
    with get_engine('read') as engine:
        query='''
		select x.fv_whname, y.fv_whtypename, fv_artsizecode, fn_qty qty, a.fd_dateinput tgl, 'Stock In Retur' as 'Stat'
		from (select fc_nodoc, fn_whto, fd_dateinput
			from returnmst_tb ) a
		inner join returndtl_tb b on a.fc_nodoc=b.fc_nodoc
		inner join warehouse_tb x on a.fn_whto=x.fn_whid
		inner join warehousetype_tb y on b.fn_whtypeid=y.fn_whtypeid

		where a.fd_dateinput > '2024-1-1' order by tgl desc'''
        df= pd.read_sql(query, engine)
    with get_engine('imp') as engine:
        df.to_sql('retur_gd_pb', engine, if_exists='replace', index=False)
    return

def so_gd_pb ():
    with get_engine('read') as engine:
        query='''
		select x.fv_whname, y.fv_whtypename, fv_artsizecode, fn_qty qty, a.fd_date tgl, 'SO Gudang' as 'Stat'
		from (select fc_nodoc, a.fn_whid, fd_date, b.fn_whtypeid
			from opnamearticlemst_tb a 
			inner join warehousetypedtl_tb b on a.fn_rtid=b.fn_rtid and a.fn_rackid=b.fn_rackid
			where fd_date > '2024-05-01 11:00:03') a
		inner join opnamearticledtl_tb b on a.fc_nodoc=b.fc_nodoc
		inner join warehouse_tb x on a.fn_whid=x.fn_whid
		inner join warehousetype_tb y on a.fn_whtypeid=y.fn_whtypeid
		where fn_qty <>0'''
        df= pd.read_sql(query, engine)
    with get_engine('imp') as engine:
        df.to_sql('so_gd_pb', engine, if_exists='replace', index=False)
    return



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

def main ():

    
    timestart_data_sales_h= datetime.now()
    print("[INFO] Script data_sales_h dimulai.")
    print(f"[INFO] Timestart: {timestart_data_sales_h}\n")
    # data_sales_h()

    timestart_data_trxstx_pb= datetime.now()
    print("[INFO] Script data_trxstx_pb dimulai.")
    print(f"[INFO] Timestart: {timestart_data_trxstx_pb}\n")
    trxstx_pb(2025)

    timestart_data_stock_gudang_pb= datetime.now()
    print("[INFO] Script data_stock_gudang_pb dimulai.")
    print(f"[INFO] Timestart: {timestart_data_stock_gudang_pb}\n")
    stock_gudang_pb() # today

    timestart_data_Stock_varian= datetime.now()
    print("[INFO] Script data_Stock_varian dimulai.")
    print(f"[INFO] Timestart: {timestart_data_Stock_varian}\n")
    Stock_varian ()

    timestart_data_trx_gudang24= datetime.now()
    print("[INFO] Script data_trx_gudang24 dimulai.")
    print(f"[INFO] Timestart: {timestart_data_trx_gudang24}\n")
    trx_gudang24()

    timestart_data_trx_retur= datetime.now()
    print("[INFO] Script data_trx_retur dimulai.")
    print(f"[INFO] Timestart: {timestart_data_trx_retur}\n")
    trx_retur()

    timestart_data_Stock_Awal_Gudang24= datetime.now()
    print("[INFO] Script data_Stock_Awal_Gudang24 dimulai.")
    print(f"[INFO] Timestart: {timestart_data_Stock_Awal_Gudang24}\n")
    Stock_Awal_Gudang24()

    timestart_data_retur_gd_pb= datetime.now()
    print("[INFO] Script data_retur_gd_pb dimulai.")
    print(f"[INFO] Timestart: {timestart_data_retur_gd_pb}\n")
    retur_gd_pb()

    timestart_data_so_gd_pb= datetime.now()
    print("[INFO] Script data_so_gd_pb dimulai.")
    print(f"[INFO] Timestart: {timestart_data_so_gd_pb}\n")
    so_gd_pb()

    timestart_data_aging= datetime.now()
    print("[INFO] Script data_aging dimulai.")
    print(f"[INFO] Timestart: {timestart_data_aging}\n")
    data_aging()


    
    timefin= datetime.now()

    #Hitung total durasi
    total_duration = calculate_duration(timestart_data_sales_h, timefin)

    #Simpan semua waktu dan durasi
    logs = [
        
        ("Start data_sales_h", timestart_data_sales_h, timestart_data_trxstx_pb),
        ("Start data_trxstx_pb", timestart_data_trxstx_pb, timestart_data_stock_gudang_pb),
        ("Start data_stock_gudang_pb", timestart_data_stock_gudang_pb, timestart_data_Stock_varian),
        ("Start data_stock_varian", timestart_data_Stock_varian, timestart_data_trx_gudang24),
        ("Start data_trx_gudang24", timestart_data_trx_gudang24, timestart_data_trx_retur),
        ("Start data_trx_retur", timestart_data_trx_retur, timestart_data_Stock_Awal_Gudang24),
        ("Start data_Stock_Awal_Gudang24", timestart_data_Stock_Awal_Gudang24, timestart_data_retur_gd_pb),
        ("Start data_retur_gd_pb", timestart_data_retur_gd_pb, timestart_data_so_gd_pb),
        ("Start data_so_gd_pb", timestart_data_so_gd_pb, timestart_data_aging),
        ("Start data_aging", timestart_data_aging, timefin),
        ("Finish", timefin, None, total_duration),
    ]

    # Menulis log ke file
    file_name = timefin.strftime("output_supp_%Y%m%d_%H%M%S.txt")
    with open(file_name, "w") as file:
        for label, start, end, *optional_duration in logs:
            formatted_start = start.strftime('%Y-%m-%d %H:%M:%S')
            if end:
                duration_formatted = calculate_duration(start, end)
                file.write(f"{label}: {formatted_start} ({duration_formatted})\n")
            else:
                # Tambahkan durasi total untuk log Finish
                total_info = f" (Total: {optional_duration[0]})" if optional_duration else ""
                file.write(f"{label}: {formatted_start}{total_info}\n")
    # # Output log ke konsol
    # for label, start, end, *optional_duration in logs:
    #     formatted_start = start.strftime('%Y-%m-%d %H:%M:%S')
    #     if end:
    #         duration_formatted = calculate_duration(start, end)
    #         print(f"{label}: {formatted_start} ({duration_formatted})")
    #     else:
    #         total_info = f" (Total: {optional_duration[0]})" if optional_duration else ""
    #         print(f"{label}: {formatted_start}{total_info}")
if __name__ == "__main__":
    main()