# transaction today

from sqlalchemy import create_engine
from contextlib import contextmanager
from datetime import date
import pandas as pd

DB_TYPE = 'mysql+pymysql'
DB_USER="dataanalyst"
DB_PASSWORD="PyData*010224"

DB_HOST1="36.93.148.91"
DB_HOST2="172.16.11.9"
DB_HOST3="192.168.1.237"
DB_PORT=3307

DB_LOCAL = "gis_local"
DB_SYNC = "gis_db1"
DB_RT = "gis_db"

@contextmanager
def get_engine(stat):
    """Context manager untuk membuka dan menutup SQLAlchemy engine."""
    if stat == 'read':
        engine = create_engine(f'{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST1}:{DB_PORT}/{DB_SYNC}')
    elif stat == 'write':
        engine = create_engine(f'{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST3}:{DB_PORT}/{DB_RT}')
    else: # 'imp'
        engine = create_engine(f'{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST2}:{DB_PORT}/{DB_LOCAL}')

    try:
        # Mengembalikan engine untuk digunakan dalam blok with
        yield engine
    finally:
        # Menutup koneksi pool engine untuk membersihkan sumber daya
        engine.dispose()

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
        # print(query)
    return df
if __name__ == "__main__":
    df__rtt()
    