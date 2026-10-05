from django.shortcuts import render
from django.db import connection
from rest_framework.views import APIView
import pandas as pd
import numpy as np
import json
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse
from datetime import datetime, timedelta
from .models import ArtikelPb, CustomersPb, TReturmstPre, TReturdtlPre
from urllib.parse import unquote
from datetime import datetime
from django.utils import timezone
from django.db import transaction
from core.db import DatabaseConnector

# Function to execute raw SQL query
def execute_raw_sql(query, params=None):
    with connection.cursor() as cursor:
        cursor.execute(query, params or [])
        columns = [col[0] for col in cursor.description]  # Get column names from cursor description
        result = cursor.fetchall()

        # Convert result to a list of dictionaries where column names are keys
        formatted_result = [dict(zip(columns, row)) for row in result]
    return formatted_result

class StockDashboardAPIView(APIView):
    dbLoad = 'default'
    dbsave = 'local'

    def __init__(self):
        pass

    def get(self, request, *args, **kwargs):
        input_value = kwargs.get('input_value')
        alokator = request.GET.get('alokator')
        store = request.GET.get('toko', [])
        start = request.GET.get('start')
        ends = request.GET.get('end')
        type = request.GET.get('type', 'default')
        format = request.GET.get('format', 'data')

        store_list = store.split(',') if store else []

        input_date = datetime.strptime(input_value, "%Y-%m-%d")  # Konversi ke datetime
        past_date = input_date - timedelta(days=90)  # Kurangi 90 hari

        start = start or input_value
        ends = ends or past_date.strftime("%Y-%m-%d")

        # Konversi ke datetime jika belum
        if isinstance(start, str):
            start = datetime.strptime(start, "%Y-%m-%d")

        if isinstance(ends, str):
            ends = datetime.strptime(ends, "%Y-%m-%d")

        # Jika hasilnya lebih kecil dari 2024-01-01, set default ke 2024-01-01
        default_date = datetime(2024, 1, 1)
        if ends < default_date:
            ends = default_date

        # Konversi kembali ke string
        start_str = start.strftime("%Y-%m-%d")
        ends_str = ends.strftime("%Y-%m-%d")

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
        WHERE 1=1
        """

        aging_query = f"""
        SELECT a.fn_whconsiid, fv_barcode, fn_stock, fd_tgltrx FROM 
        (SELECT * FROM gis_local.trxstx_pb24 
        where fv_stat like 'receive%%' and fd_tgltrx <= '{input_value}'
        union all
        SELECT * FROM gis_local.trxstx_pb25 
        where fv_stat like 'receive%%' and fd_tgltrx <= '{input_value}') a
        left join artikel_pb b on a.fv_artsizecode = b.fv_artsizecode
        LEFT JOIN customers_pb c ON c.fn_whconsiid = a.fn_whconsiid
        WHERE 1=1        
        """

        sales_query = f"""
        select fv_barcode, sum(fn_jualpersize) as sales, fn_jualpersize, fv_sizename, fv_toko, trx.fv_artsizecode, fc_code from (
        SELECT fv_barcode, fn_jualpersize, fd_tgltrx, fv_alokator, fc_code, fv_sizename, concat(fc_code, ' - ', fv_toko) fv_toko, a.fv_artsizecode FROM gis_local.pb_penjualan_2025 a
        left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
        left join customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        union all 
        SELECT fv_barcode, fn_jualpersize, fd_tgltrx, fv_alokator, fc_code, fv_sizename, concat(fc_code, ' - ', fv_toko) fv_toko, a.fv_artsizecode FROM gis_local.pb_penjualan_2024 a
        left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        left join customers_pb c on a.fn_whconsiid=c.fn_whconsiid) trx
        where trx.fd_tgltrx between '{ends_str}' and '{start_str}'                      
        """

        retur_query = f"""
        select a.fn_whconsiid, fv_barcode, sum(qty)*-1 as retur FROM gis_local.trx_retur a
        left join artikel_pb b on a.fv_artsizecode=b.fv_artsizecode 
        left join customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        WHERE tgltrx <= '{input_value}' 
        """

        mutasi_query = f"""SELECT a.fn_whconsiid, fv_barcode, sum(fn_qtykirim) Mutasi FROM 
        (SELECT fn_whconsiid, fv_artsizecode,fd_tgltransaksi, fn_qtykirim from gis_local.dt_mutasi_2025 
        where fv_userbatal is null and fd_tgltransaksi <= '{input_value}'
        union all  
        SELECT fn_whconsiid, fv_artsizecode,fd_tgltransaksi, fn_qtykirim from gis_local.dt_mutasi_2024
        where fv_userbatal is null and fd_tgltransaksi <= '{input_value}') a
        left join  gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        left join customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        where 1=1
        """

        OutstandingDO_query =  f'''SELECT 
            fn_whconsiid ,fv_artsizecode, sum(fn_qty) OstDO
            FROM gis_db1.dodtl_tb a 
            inner join gis_db1.domst_tb b on a.fc_nodoc=b.fc_nodoc
            inner join gis_db1.t_ordermst c on b.fc_noreff=fv_noorder
            left join gis_db1.t_receivingmst d on b.fc_nodoc=d.fc_nosj
            left join gis_db1.warehouseconsi_tb e on c.fn_cusid=e.fn_cusid
            where  fv_noreceiving is null and fd_date <= '{input_value}'and b.fc_status="F" 
            group by fn_whconsiid, fv_artsizecode, fd_date '''
        # f"""SELECT a.fn_whconsiid, fv_barcode, sum(fn_qty) OstDO FROM gis_local.dt_OSDO a
        # left join  gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        # left join customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        # where fd_date <= '{input_value}' 
        # """

       

        OutstandingDM_query = f'''SELECT 
            fn_whconsiid2, fv_artsizecode, sum(fn_qty) OstDM 
            FROM gis_db1.dodtl_tb a 
            inner join gis_db1.domst_tb b on a.fc_nodoc=b.fc_nodoc
            inner join gis_db1.t_mutasimst c on b.fc_noreff=c.fv_nomutasi
            left join gis_db1.t_receivingmst d on b.fc_nodoc=d.fc_nosj
            where  fv_noreceiving is null and fd_date <= '{input_value}' 
            and b.fc_status="F" 
            group by fn_whconsiid, fv_artsizecode, fd_date '''
        # f"""SELECT c.fn_whconsiid, fv_barcode, sum(fn_qty) OstDM FROM gis_local.dt_OSDM a
        # left join  gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        # left join customers_pb c on a.fn_whconsiid2=c.fn_whconsiid
        # where fd_date <= '{input_value}' 
        # """
        
        
        obdo_query= f"""SELECT a.fn_whconsiid, fv_barcode, fd_ob, sum(fn_orderpersize) qty_ob, fd_do, sum(fn_qtyterkirim) qty_do FROM gis_local.dt_OB_DO a 
        left join  gis_local.artikel_pb b on a.fv_artsizecode=b.fv_artsizecode
        left join customers_pb c on a.fn_whconsiid=c.fn_whconsiid
        where 1=1 """

        art_query = '''SELECT * from artikel_pb'''

        if alokator:
            main_query += f"AND b.fv_alokator='{alokator}' "
            aging_query += f"AND c.fv_alokator='{alokator}' "
            sales_query += f"and fv_alokator='{alokator}' "
            retur_query += f"and fv_alokator='{alokator}' "
            mutasi_query += f"and fv_alokator='{alokator}' "
            # OutstandingDO_query += f"and fv_alokator='{alokator}' "
            # OutstandingDM_query += f"and fv_alokator='{alokator}' "
            obdo_query += f"and fv_alokator='{alokator}' "
            

        if store_list:
            placeholders =  ", ".join([f"'{s}'" for s in store_list])
            main_query += f"AND b.fc_code in ({placeholders}) "
            aging_query += f"AND c.fc_code in ({placeholders}) "
            sales_query += f"AND fc_code in ({placeholders}) "
            retur_query += f"AND fc_code in ({placeholders}) "
            mutasi_query += f"AND c.fc_code in ({placeholders}) "
            # OutstandingDO_query += f"AND c.fc_code in ({placeholders}) "
            # OutstandingDM_query += f"AND c.fc_code in ({placeholders}) "
            obdo_query += f"AND c.fc_code in ({placeholders}) "

        main_query += "GROUP BY fn_whconsiid, fv_artsizecode"
        aging_query += "group by fn_whconsiid,fv_barcode, fd_tgltrx"
        if type == 'default':
            sales_query += f"""
                            group by fv_barcode, fc_code, fv_toko
                            """
        else:
            sales_query += f"""
                            group by trx.fv_artsizecode, fv_toko, fc_code
                            """
        retur_query += f"""
        group by fv_toko, fv_barcode
        """
        mutasi_query += f""" group by fn_whconsiid, fv_barcode"""
        # OutstandingDO_query += f""" group by fn_whconsiid, fv_barcode"""
        # OutstandingDM_query += f""" group by fn_whconsiid, fv_barcode"""
        obdo_query += f""" group by fn_whconsiid, fv_barcode"""

        # print(main_query)
        results = DatabaseConnector.execute_raw_sql(main_query, using=StockDashboardAPIView.dbLoad)
        aging_results = execute_raw_sql(aging_query)
        sales_results = execute_raw_sql(sales_query)
        retur_results = execute_raw_sql(retur_query)
        mutasi_results = execute_raw_sql(mutasi_query)
        OutstandingDO_results = DatabaseConnector.execute_raw_sql(OutstandingDO_query, using=StockDashboardAPIView.dbsave)
        OutstandingDM_results = DatabaseConnector.execute_raw_sql(OutstandingDM_query, using=StockDashboardAPIView.dbsave)
        obdo_results = execute_raw_sql(obdo_query)
        art_result = execute_raw_sql(art_query)
        if format == 'data':
            #Default API or Detail API
            if type == 'default':
                # Convert results to DataFrame
                df = pd.DataFrame(results)

                # Convert aging_results to DataFrame
                aging_df = pd.DataFrame(aging_results)

                # Convert sales_results to DataFrame
                sales_df = pd.DataFrame(sales_results)

                # Convert retur_results to DataFrame
                retur_df = pd.DataFrame(retur_results)

                # Convert mutasi_results to DataFrame
                mutasi_df = pd.DataFrame(mutasi_results)

                # Convert OutstandingDO_results to DataFrame
                OutstandingDO_df = pd.DataFrame(OutstandingDO_results)
                
                # Convert OutstandingDO_results to DataFrame
                OutstandingDM_df = pd.DataFrame(OutstandingDM_results)

                # Convert obdo_results to DataFrame
                obdo_df = pd.DataFrame(obdo_results)

                art_df = pd.DataFrame(art_result)
                #pd.set_option('future.no_silent_downcasting', True)

                # Jika aging_df tidak kosong, lakukan perhitungan dan merge
                if not aging_df.empty:
                    # Group aging_df by fn_whconsiid and fv_barcode, and get the minimum fd_tgltrx
                    aging_min_date = aging_df.groupby(['fn_whconsiid', 'fv_barcode'])['fd_tgltrx'].min().reset_index()
                    aging_min_date.rename(columns={'fd_tgltrx': '1st DO'}, inplace=True)

                    # Group aging_df by fn_whconsiid and fv_barcode, and get the maximum fd_tgltrx (last_receive)
                    aging_max_date = aging_df.groupby(['fn_whconsiid', 'fv_barcode'])['fd_tgltrx'].max().reset_index()
                    aging_max_date.rename(columns={'fd_tgltrx': 'last_receive'}, inplace=True)

                    # Merge df with aging_min_date on fn_whconsiid and fv_barcode
                    df = pd.merge(df, aging_min_date, on=['fn_whconsiid', 'fv_barcode'], how='left')
                    df = pd.merge(df, aging_max_date, on=['fn_whconsiid', 'fv_barcode'], how='left')

                    # Get stock on 1st DO
                    # Merge aging_df with aging_min_date to get stock on 1st DO
                    aging_stock_on_1st_do = pd.merge(
                        aging_df,
                        aging_min_date,
                        on=['fn_whconsiid', 'fv_barcode'],
                        how='left'
                    )

                    # Filter rows where fd_tgltrx matches the 1st DO
                    aging_stock_on_1st_do = aging_stock_on_1st_do[
                        aging_stock_on_1st_do['fd_tgltrx'] == aging_stock_on_1st_do['1st DO']]

                    # Select only the required columns
                    aging_stock_on_1st_do = aging_stock_on_1st_do[['fn_whconsiid', 'fv_barcode', 'fn_stock']]
                    aging_stock_on_1st_do.rename(columns={'fn_stock': 'stock_on_1st_do'}, inplace=True)

                    # Merge df with aging_stock_on_1st_do to add stock_on_1st_do column
                    df = pd.merge(df, aging_stock_on_1st_do, on=['fn_whconsiid', 'fv_barcode'], how='left')
                else:
                    # Jika aging_df kosong, buat kolom dengan nilai default
                    df['1st DO'] = '2024-01-01'
                    df['last_receive'] = '2024-01-01'
                    df['aging'] = '2024-01'
                    df['stock_on_1st_do'] = 0

                # Fill NaN values in stock_on_1st_do with 0 (if no stock data on 1st DO)
                df['stock_on_1st_do'] = df['stock_on_1st_do'].fillna(0)

                # Convert '1st DO' to datetime
                df['1st DO'] = pd.to_datetime(df['1st DO'])
                df['last_receive'] = pd.to_datetime(df['last_receive'])

                # Hitung aging (selisih hari dari last_receive ke hari ini)
                df['aging'] = (pd.Timestamp.today().normalize() - df['last_receive']).dt.days.fillna(0).astype(int)

                # Format '1st DO' to 'YYYY-MM-DD'
                df['1st DO'] = df['1st DO'].dt.strftime('%Y-%m-%d')
                df['last_receive'] = df['last_receive'].dt.strftime('%Y-%m-%d')



                # Daftar semua kemungkinan ukuran yang ingin ditampilkan
                all_sizes_bottom = ['25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38',
                                    '39', '40', '42', '44', 'ALLSIZE']
                all_sizes_top = ['FS', 'XS', 'SS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL']

                # Create pivot table
                pivot_table = pd.pivot_table(
                    df,
                    values='stocks',  # Nilai yang akan di-aggregate
                    index=['fv_toko', 'fv_barcode','fn_whconsiid'],
                    # Baris pivot , 'fv_catname', 'fv_brandname', 'fv_configname', 'fv_namemark', 'fm_price', 'fv_divname', 'fv_rating', '1st DO', 'last_receive', 'aging', 'fv_colorname', 'stock_on_1st_do'
                    columns=['fv_sizename'],  # Kolom pivot
                    aggfunc='sum',  # Fungsi aggregate (sum, mean, count, etc.)
                    fill_value=0  # Nilai default jika data tidak ada
                )

                kolom = ['fv_toko', 'fv_barcode', 'fv_catname', 'fv_brandname', 'fv_configname', 'fv_namemark', 'fm_price',
                         'fv_divname', 'fv_rating', '1st DO', 'last_receive', 'aging', 'fv_colorname', 'stock_on_1st_do','fn_whconsiid']
                pivot_table = pd.merge(pivot_table, df[kolom], on=['fv_toko', 'fv_barcode'], how='left')
                pivot_table = pivot_table.drop_duplicates()

                kolom1 = kolom + [col for col in pivot_table.columns if col not in kolom]
                pivot_table = pivot_table[kolom1]

                all_sizes = all_sizes_bottom + all_sizes_top

                # Pastikan semua ukuran muncul meskipun tidak ada dalam data asli
                for size in all_sizes:
                    if size not in pivot_table.columns:
                        pivot_table[size] = 0  # Tambahkan kolom dengan nilai default 0
                    else:
                        # Konversi kolom ke tipe numerik dan isi NaN dengan 0
                        pivot_table[size] = pd.to_numeric(pivot_table[size], errors='coerce').fillna(0)

                        # Konversi ke tipe integer
                    pivot_table[size] = pivot_table[size].astype(int)

                # **Pastikan indeks tetap rapi**
                # pivot_table = pivot_table.reindex(columns=all_sizes, fill_value=0)

                # df1 = pivot_table.groupby('fv_toko')['stocks'].sum()

                # **Menambahkan kolom `status`**
                size_columns = [size for size in all_sizes if
                                size in pivot_table.columns]  # Pastikan hanya ukuran yang valid
                other_columns = [col for col in pivot_table.columns if col not in all_sizes]  # Kolom selain ukuran

                # Gabungkan kolom lainnya dengan kolom ukuran yang sudah diurutkan
                pivot_table = pivot_table[other_columns + size_columns]

                # Hapus indeks
                pivot_table = pivot_table.reset_index(drop=True)

                top_columns = [ "L", "M", "S", "XL", "XS", "XXL", "XXXL"]
                bottom_columns = ["25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "42", "44"]
                pivot_table["status"] = np.where(
                    (pivot_table["fv_configname"] == "TOP") & (pivot_table[top_columns].gt(1).sum(axis=1) < 3), "BROKEN", 
                    np.where(
                        (pivot_table["fv_configname"] == "BOTTOM") & (pivot_table[bottom_columns].gt(1).sum(axis=1) < 5), "BROKEN","SEHAT"
                    ))

                # **Reset index agar status terlihat sebagai kolom**
                pivot_table = pivot_table.reset_index()

                pivot_table['sumQty'] = pivot_table.iloc[:, 16:43].fillna(0).astype(int).sum(axis=1)

                # ambil data csv
                csv_df = pd.read_csv(r'\\192.168.1.202\phytonapi\ornal.csv')

                if not csv_df.empty:
                    # merge with csv
                    pivot_table = pd.merge(pivot_table, csv_df, left_on='fv_barcode', right_on='artikel', how='left')
                else:
                    pivot_table['Ornal'] = ''

                # sum stock ke samping
                # pivot_table['sumQty']=pivot_table.iloc[:, 14:41].sum(axis=1)
                # pivot_table['sumQty'] = 0
                # df1 = pivot_table.groupby('fv_toko')['sumQty'].sum()

                # merge with sales
                pivot_table = pd.merge(pivot_table, sales_df[['fv_barcode', 'sales', 'fv_toko', 'fc_code']], on=['fv_barcode', 'fv_toko'], how='left')

                # Fill NaN values in 'sales' column with 0
                pivot_table['sales'] = pivot_table['sales'].fillna(0)

                # stock awal
                # Konversi kolom pada indeks 44 dan 47 ke tipe numerik
                pivot_table.iloc[:, 45] = pd.to_numeric(pivot_table.iloc[:, 45], errors='coerce')
                pivot_table.iloc[:, 48] = pd.to_numeric(pivot_table.iloc[:, 48], errors='coerce')

                # Isi nilai NaN dengan 0 (jika ada nilai yang tidak bisa dikonversi)
                pivot_table.iloc[:, 45] = pivot_table.iloc[:, 45].fillna(0)
                pivot_table.iloc[:, 48] = pivot_table.iloc[:, 48].fillna(0)

                # Jumlahkan kolom pada indeks 44 dan 47
                pivot_table['stockAwal'] = pivot_table.iloc[:, 45] + pivot_table.iloc[:, 48]

                # Hapus indeks dan pastikan tidak ada kolom 'index'
                pivot_table = pivot_table.reset_index(drop=True)

                # Add retur
                if not retur_df.empty:
                    # merge with retur
                    pivot_table = pd.merge(pivot_table, retur_df, on=['fv_barcode','fn_whconsiid'], how='left')

                    # Isi nilai NaN di kolom retur dengan 0
                    pivot_table['retur'] = pivot_table['retur'].fillna(0)
                else:

                    pivot_table['retur'] = 0

                # Add Mutasi
                if not mutasi_df.empty:
                    # merge with retur
                    pivot_table = pd.merge(pivot_table, mutasi_df, on=['fv_barcode','fn_whconsiid'], how='left')

                    # Isi nilai NaN di kolom retur dengan 0
                    pivot_table['Mutasi'] = pivot_table['Mutasi'].fillna(0)
                else:

                    pivot_table['Mutasi'] = 0

                # Add OutstandingDO
                if not OutstandingDO_df.empty:
                    # merge with retur
                    def df__customers_pb_n(placeholders):
                        query = f'''
                            SELECT fn_cusid,fn_whconsiid FROM gis_local.customers_pb
                            where fc_code in ({placeholders})
                            '''
                        results = execute_raw_sql(query)
                        df = pd.DataFrame(results)
                        return df
                    df_cus=df__customers_pb_n(placeholders)
                    cus_list= df_cus['fn_cusid'].tolist()
                    cus_list =  ", ".join([f"'{s}'" for s in cus_list])
                    def change_cusid_to_whconsiid(df):
                        df_cus=df__customers_pb_n(placeholders)
                        df=pd.merge(df,df_cus,how='left',on='fn_cusid')
                        df['fn_cusid']=df['fn_whconsiid']
                        df=df.drop(columns='fn_whconsiid')
                        df=df.rename(columns={'fn_cusid':'fn_whconsiid'})
                        return df

                    def get_ostdo_(cus_list):
                        query = f'''
                            SELECT fv_artsizecode,fv_barcode from artikel_pb
                            '''
                        results = execute_raw_sql(query)
                        df_art = pd.DataFrame(results)
                        query= f'''SELECT 
                        c.fn_cusid ,fv_artsizecode, sum(fn_qty) fn_qty
                        FROM gis_db.dodtl_tb a 
                        inner join gis_db.domst_tb b on a.fc_nodoc=b.fc_nodoc
                        inner join gis_db.t_ordermst c on b.fc_noreff=fv_noorder
                        left join gis_db.t_receivingmst d on b.fc_nodoc=d.fc_nosj
                        where  fv_noreceiving is null and c.fn_cusid in ({cus_list}) and b.fc_status="F" 
                        group by fn_cusid, fv_artsizecode, fd_date 
                        '''
                        # DatabaseConnector.execute_raw_sql(OutstandingDO_query, using=StockDashboardAPIView.dbsave)
                        results = DatabaseConnector.execute_raw_sql(query, using=StockDashboardAPIView.dbsave)
                        df = pd.DataFrame(results)
                        df=change_cusid_to_whconsiid(df)
                        df=pd.merge(df, df_art, on='fv_artsizecode', how='left')
                        df=df.groupby(['fn_whconsiid','fv_barcode',], as_index=False)['fn_qty'].sum()
                        return df
                    OutstandingDO_df=get_ostdo_(cus_list)
                    pivot_table = pd.merge(pivot_table, OutstandingDO_df, on=['fv_barcode','fn_whconsiid'], how='left')

                    # Isi nilai NaN di kolom retur dengan 0
                    pivot_table['OstDO'] = pivot_table['OstDO'].fillna(0)
                else:

                    pivot_table['OstDO'] = 0

                # Add obdo
                if not obdo_df.empty:
                    # merge with retur
                    pivot_table = pd.merge(pivot_table, obdo_df, on=['fv_barcode','fn_whconsiid'], how='left')

                    # Isi nilai NaN di kolom retur dengan 0
                    pivot_table['qty_ob'] = pivot_table['qty_ob'].fillna(0)
                    pivot_table['qty_do'] = pivot_table['qty_do'].fillna(0)
                else:

                    pivot_table['qty_ob'] = 0
                    pivot_table['qty_do'] = 0
                df=pivot_table

                # Convert '1st DO' to datetime
                df['fd_ob'] = pd.to_datetime(df['fd_ob'])
                df['fd_do'] = pd.to_datetime(df['fd_do'])

                # Format '1st DO' to 'YYYY-MM-DD'
                df['fd_ob'] = df['fd_ob'].dt.strftime('%Y-%m-%d')
                df['fd_do'] = df['fd_do'].dt.strftime('%Y-%m-%d')

                # Fill NaN values in stock_on_1st_do with 0 (if no stock data on 1st DO)
                df['fd_ob'] = df['fd_ob'].fillna('-')
                df['fd_do'] = df['fd_do'].fillna('-')
                pivot_table=df
                
                # Add OutstandingDO
                if not OutstandingDM_df.empty:
                    # merge with retur
                    def df__customers_pb_n(placeholders):
                        query = f'''
                            SELECT fn_cusid,fn_whconsiid FROM gis_local.customers_pb
                            where fc_code in ({placeholders})
                            '''
                        results = execute_raw_sql(query)
                        df = pd.DataFrame(results)
                        return df
                    df_cus=df__customers_pb_n(placeholders)
                    cus_list= df_cus['fn_cusid'].tolist()
                    cus_list =  ", ".join([f"'{s}'" for s in cus_list])
                    def change_cusid_to_whconsiid(df):
                        df_cus=df__customers_pb_n(placeholders)
                        df=pd.merge(df,df_cus,how='left',on='fn_cusid')
                        df['fn_cusid']=df['fn_whconsiid']
                        df=df.drop(columns='fn_whconsiid')
                        df=df.rename(columns={'fn_cusid':'fn_whconsiid'})
                        return df

                    def get_ostdm_(whconsi_list):
                        query = f'''
                            SELECT fv_artsizecode,fv_barcode from artikel_pb
                            '''
                        results = execute_raw_sql(query)
                        df_art = pd.DataFrame(results)
                        query= f'''
                            SELECT 
                                fn_whconsiid2 fn_whconsiid,fv_artsizecode, sum(fn_qty) fn_qty
                                FROM gis_db.dodtl_tb a 
                                inner join gis_db.domst_tb b on a.fc_nodoc=b.fc_nodoc
                                inner join gis_db.t_mutasimst c on b.fc_noreff=c.fv_nomutasi
                                left join gis_db.t_receivingmst d on b.fc_nodoc=d.fc_nosj
                                where  fv_noreceiving is null and fn_whconsiid2 in ({whconsi_list})
                                and b.fc_status="F" 
                                group by fn_whconsiid2, fv_artsizecode, fd_date 
                            '''
                        results = DatabaseConnector.execute_raw_sql(query, using=StockDashboardAPIView.dbsave)
                        df = pd.DataFrame(results)
                        # df=change_cusid_to_whconsiid(df)
                        df=pd.merge(df, df_art, on='fv_artsizecode', how='left')
                        df=df.groupby(['fn_whconsiid','fv_barcode',], as_index=False)['fn_qty'].sum()
                        return df
                    OutstandingDM_df=get_ostdm_(cus_list)
                    pivot_table = pd.merge(pivot_table, OutstandingDM_df, on=['fv_barcode','fn_whconsiid'], how='left')

                    # Isi nilai NaN di kolom retur dengan 0
                    pivot_table['OstDM'] = pivot_table['OstDM'].fillna(0)
                else:

                    pivot_table['OstDM'] = 0

                
                pivot_table=pivot_table.drop(columns='fn_whconsiid')
                # Convert pivot table to JSON
                pivot_json = pivot_table.to_json(orient='split')

                # Parse JSON string ke dictionary agar dikembalikan sebagai JSON yang valid
                pivot_dict = json.loads(pivot_json)

                # If no results found
                if not results:
                    return Response({"message": "No data found"}, status=status.HTTP_204_NO_CONTENT)
                else:
                    return JsonResponse(pivot_dict, safe=False)
            else:
                # Convert results to DataFrame
                df = pd.DataFrame(sales_results)

                # Daftar semua kemungkinan ukuran yang ingin ditampilkan
                all_sizes_bottom = ['25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38',
                                    '39', '40', '42', '44', 'ALLSIZE']
                all_sizes_top = ['FS', 'XS', 'SS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL']

                # Create pivot table
                pivot_table = pd.pivot_table(
                    df,
                    values='sales',  # Nilai yang akan di-aggregate
                    index=['fv_toko', 'fv_barcode'],
                    # Baris pivot
                    columns=['fv_sizename'],  # Kolom pivot
                    aggfunc='sum',  # Fungsi aggregate (sum, mean, count, etc.)
                    fill_value=0  # Nilai default jika data tidak ada
                )

                kolom = ['fv_barcode', 'fv_toko']
                pivot_table = pd.merge(pivot_table, df[kolom], on=['fv_toko', 'fv_barcode'], how='left')
                pivot_table = pivot_table.drop_duplicates()

                kolom1 = kolom + [col for col in pivot_table.columns if col not in kolom]
                pivot_table = pivot_table[kolom1]

                all_sizes = all_sizes_bottom + all_sizes_top

                # Pastikan semua ukuran muncul meskipun tidak ada dalam data asli
                for size in all_sizes:
                    if size not in pivot_table.columns:
                        pivot_table[size] = 0  # Tambahkan kolom dengan nilai default 0
                    else:
                        # Konversi kolom ke tipe numerik dan isi NaN dengan 0
                        pivot_table[size] = pd.to_numeric(pivot_table[size], errors='coerce').fillna(0)

                        # Konversi ke tipe integer
                    pivot_table[size] = pivot_table[size]

                # **Pastikan indeks tetap rapi**
                # pivot_table = pivot_table.reindex(columns=all_sizes, fill_value=0)

                # df1 = pivot_table.groupby('fv_toko')['stocks'].sum()

                # **Menambahkan kolom `status`**
                size_columns = [size for size in all_sizes if
                                size in pivot_table.columns]  # Pastikan hanya ukuran yang valid
                other_columns = [col for col in pivot_table.columns if col not in all_sizes]  # Kolom selain ukuran

                # Gabungkan kolom lainnya dengan kolom ukuran yang sudah diurutkan
                pivot_table = pivot_table[other_columns + size_columns]

                # Hapus indeks
                pivot_table = pivot_table.reset_index(drop=True)


                # Convert pivot table to JSON
                pivot_json = pivot_table.to_json(orient='split')

                # Parse JSON string ke dictionary agar dikembalikan sebagai JSON yang valid
                pivot_dict = json.loads(pivot_json)

                # If no results found
                if not results:
                    return Response({"message": "No data found"}, status=status.HTTP_204_NO_CONTENT)
                else:
                    return JsonResponse(pivot_dict, safe=False)
        else:
            return 0

    def post(self, request, *args, **kwargs):
        input_value = kwargs.get('input_value')
        toko = request.data.get("toko")
        keterangan = request.data.get("keterangan")
        data = request.data.get("data")

        if input_value:
            try:
                # 1. Decode string URL (ganti %20 dengan spasi)
                decoded_time_str = unquote(input_value)  # hasil: "2025-06-05 02:58:32"

                # 2. Parse ke datetime object
                only_date_str = decoded_time_str.split(" ")[0]
                dt = datetime.strptime(only_date_str, "%Y-%m-%d")

                # 3. Ambil bulan dan tahun
                bulan = dt.strftime('%m')
                tahun = dt.strftime('%y')
                prefix = f'RT{bulan}{tahun}'

                # 4. Hitung jumlah entri yang sudah ada di bulan-tahun tersebut
                count = TReturmstPre.objects.filter(
                    fd_tglretur__year=dt.year,
                    fd_tglretur__month=dt.month
                ).count() + 1

                # 5. Format urutan
                nomor_urut = str(count).zfill(3)

                # 6. Bangun kode
                self.kode = f"{prefix}-{nomor_urut}"
                print(self.kode)

            except Exception as e:
                print("Gagal parsing input_value:", e)

        if not data:
            return Response({"message": "No data found"}, status=status.HTTP_204_NO_CONTENT)

        #get store id
        store_obj = CustomersPb.objects.get(fc_code=toko)
        if not store_obj:
            return Response({"message": "Store not found"}, status=status.HTTP_404_NOT_FOUND)

        store_id = store_obj.fn_whconsiid
        print(store_id)
        #pivot size
        UKURAN_LIST = [
            "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35",
            "36", "37", "38", "39", "40", "42", "44", "ALLSIZE", "FS", "XS",
            "SS", "S", "M", "L", "XL", "XXL", "XXXL"
        ]

        hasil_pivot = []

        for row in data:
            if not row:
                continue
            artikel = row[0]
            jumlah_list = row[1:]
            for idx, jumlah in enumerate(jumlah_list):
                if idx < len(UKURAN_LIST):
                    ukuran = UKURAN_LIST[idx]
                    try:
                        jumlah_int = int(jumlah)
                        if jumlah_int > 0:
                            try:
                                artikel_obj = ArtikelPb.objects.get(fv_barcode=artikel, fv_sizename=ukuran)
                                artsizecode = artikel_obj.fv_artsizecode
                                articleid = artikel_obj.fn_articleid
                            except ArtikelPb.DoesNotExist:
                                artsizecode = None
                                articleid = None

                            hasil_pivot.append({
                                "artikel": artikel,
                                "ukuran": ukuran,
                                "jumlah": jumlah_int,
                                "fv_artsizecode": artsizecode,
                                "fn_articleid": articleid
                            })
                    except ValueError:
                        continue  # Lewatkan jika tidak bisa diubah ke int
        print(hasil_pivot)

        try:
            with transaction.atomic():
                # Simpan ke TReturmstPre
                header = TReturmstPre.objects.create(
                    fv_noretur=self.kode,
                    fv_noresikurir=None,
                    fd_tglretur=input_value,
                    fn_whconsiid=store_id,
                    fv_keteranganumum=keterangan,
                    fc_status='T'  # atau sesuai default/status awal
                )

                # Simpan ke TReturdtlPre
                for item in hasil_pivot:
                    TReturdtlPre.objects.create(
                        fv_noretur=self.kode,
                        fn_articleid=item.get("fn_articleid"),
                        fv_artsizecode=item.get("fv_artsizecode"),
                        fn_qtykirim=item.get("jumlah"),
                        fc_status='T'  # atau status awal lainnya
                    )
        except Exception as e:
            print("Gagal menyimpan ke database:", e)
            return Response({"message": "Failed to save data", "error": str(e)},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({"message": "Data berhasil disimpan", "kode": self.kode}, status=status.HTTP_201_CREATED)
