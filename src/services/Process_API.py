import pandas as pd
import numpy as np
from core.db import DatabaseConnector
from services.Queries_API import (df__customers_pb_n,df__customers_pb,df_artikel_pb_,df__main,df__aging,df__sales,
                                  df__retur,df__mutasi,df__obdo,change_cusid_to_whconsiid,df_ostdo_,df_ostdm_)


def proc_add_aging(df,df_aging):
### Proses untuk menambahkan kolom '1st DO', 'last_receive', dan 'stock_on_1st_do' ke DataFrame df.
    if not df.empty:
        # Group df_aging by fn_whconsiid and fv_barcode, and get the minimum fd_tgltrx
        aging_min_date = df_aging.groupby(['fn_whconsiid', 'fv_barcode'])['fd_tgltrx'].min().reset_index()
        aging_min_date.rename(columns={'fd_tgltrx': '1st DO'}, inplace=True)

        # Group df_aging by fn_whconsiid and fv_barcode, and get the maximum fd_tgltrx (last_receive)
        aging_max_date = df_aging.groupby(['fn_whconsiid', 'fv_barcode'])['fd_tgltrx'].max().reset_index()
        aging_max_date.rename(columns={'fd_tgltrx': 'last_receive'}, inplace=True)

        # Merge df with aging_min_date on fn_whconsiid and fv_barcode
        df = pd.merge(df, aging_min_date, on=['fn_whconsiid', 'fv_barcode'], how='left')
        df = pd.merge(df, aging_max_date, on=['fn_whconsiid', 'fv_barcode'], how='left')

        # Get stock on 1st DO
        # Merge aging_df with aging_min_date to get stock on 1st DO
        aging_stock_on_1st_do = pd.merge(
            df_aging,
            aging_min_date,
            on=['fn_whconsiid', 'fv_barcode'],
            how='left'
        )

        # Filter rows where fd_tgltrx matches the 1st DO
        aging_stock_on_1st_do = aging_stock_on_1st_do[
        aging_stock_on_1st_do['fd_tgltrx'] == aging_stock_on_1st_do['1st DO']]

        # Select only the required columns
        aging_stock_on_1st_do = aging_stock_on_1st_do[['fn_whconsiid', 'fv_barcode', 'fn_stock']]
        # aging_stock_on_1st_do.rename(columns={'fn_stock': 'stock_on_1st_do'}, inplace=True)

        # Merge df with aging_stock_on_1st_do to add stock_on_1st_do column
        df = pd.merge(df, aging_stock_on_1st_do, on=['fn_whconsiid', 'fv_barcode'], how='left')
        df_sum = df.groupby(['fn_whconsiid', 'fv_barcode'], as_index=False)['fn_stock'].sum().drop_duplicates(subset=['fv_barcode'])
        df = pd.merge(df, df_sum, on=['fn_whconsiid', 'fv_barcode'], how='left')
        df = df.rename(columns={'fn_stock_y': 'stock_on_1st_do'}).drop(columns=['fn_stock_x'])
        df['stock_on_1st_do'] = df['stock_on_1st_do'].fillna(0)
        # Convert '1st DO' to datetime
        df['1st DO'] = pd.to_datetime(df['1st DO'])
        df['last_receive'] = pd.to_datetime(df['last_receive'])

        # Hitung aging (selisih hari dari last_receive ke hari ini)
        df['aging'] = (pd.Timestamp.today().normalize() - df['last_receive']).dt.days.fillna(0).astype(int)

        # Format '1st DO' to 'YYYY-MM-DD'
        df['1st DO'] = df['1st DO'].dt.strftime('%Y-%m-%d')
        df['last_receive'] = df['last_receive'].dt.strftime('%Y-%m-%d')
        return df
    else:
        # Jika aging_df kosong, buat kolom dengan nilai default
        df['1st DO'] = '2024-01-01'
        df['last_receive'] = '2024-01-01'
        df['aging'] = '2024-01'
        df['stock_on_1st_do'] = 0
        return df

def proc_pivot_table(df):
    all_sizes_bottom = ['25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38',
                        '39', '40', '42', '44', 'ALLSIZE']
    all_sizes_top = ['FS', 'XS', 'SS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL']
    all_sizes = all_sizes_bottom + all_sizes_top

    # Buat pivot table
    pivot_table = pd.pivot_table(
        df,
        values='stocks',
        index=['fv_barcode', 'fn_whconsiid'],
        columns=['fv_sizename'],
        aggfunc='sum',
        fill_value=0
    )

    # Reset index agar bisa merge
    pivot_table = pivot_table.reset_index()

    # Kolom detail yang ingin ditambahkan kembali
    kolom_detail = ['fv_barcode', '1st DO', 'last_receive', 'aging','fv_configname',
                    'stock_on_1st_do', 'fn_whconsiid']
    # Merge detail
    pivot_table = pd.merge(pivot_table, df[kolom_detail], on=['fv_barcode', 'fn_whconsiid'], how='left')
    pivot_table = pivot_table.drop_duplicates()

    # Urutkan kolom agar kolom ukuran di akhir
    urutan_kolom = kolom_detail + [col for col in pivot_table.columns if col not in kolom_detail]
    pivot_table = pivot_table[urutan_kolom]

    # Pastikan semua ukuran muncul
    for size in all_sizes:
        if size not in pivot_table.columns:
            pivot_table[size] = 0
        pivot_table[size] = pd.to_numeric(pivot_table[size], errors='coerce').fillna(0).astype(int)

    # Atur ulang urutan: non-size columns dulu, lalu size columns
    size_columns = [size for size in all_sizes if size in pivot_table.columns]
    other_columns = [col for col in pivot_table.columns if col not in size_columns]
    pivot_table = pivot_table[other_columns + size_columns]

    # Hitung status (BROKEN / SEHAT)
    top_columns = ["L", "M", "S", "XL", "XS", "XXL", "XXXL"]
    bottom_columns = ['25', '26', '27', '28', '29', '30', '31', '32', '33', '34',
                        '35', '36', '37', '38', '39', '40', '42', '44']

    pivot_table['status'] = np.where(
        (pivot_table["fv_configname"] == "TOP") & (pivot_table[top_columns].gt(0).sum(axis=1) < 3), "BROKEN",
        np.where(
            (pivot_table["fv_configname"] == "BOTTOM") & (pivot_table[bottom_columns].gt(0).sum(axis=1) < 5), "BROKEN",
            "SEHAT"
        )
    )

    # Hitung total jumlah qty size (sumQty)
    pivot_table['sumQty'] = pivot_table[size_columns].sum(axis=1)
    pivot_table=pivot_table.drop(columns=['fv_configname'])

    return pivot_table

def proc_add_ornal(df):
    # ambil data csv
    csv_df = pd.read_csv(r'\\192.168.1.202\phytonapi\ornal.csv')

    if not csv_df.empty:
        # merge with csv
        df = pd.merge(df, csv_df, left_on='fv_barcode', right_on='artikel', how='left')
        df = df.drop(columns=['artikel'])
    else:
        df['Ornal'] = ''
    return df

def proc_sales_stock(df, df_sales):
    # Merge df with df_sales on 'fv_barcode' and 'fn_whconsiid'
    df = pd.merge(df, df_sales[['fv_barcode', 'sales', 'fn_whconsiid']], on=['fv_barcode', 'fn_whconsiid'], how='left')
    df['sales'] = df['sales'].fillna(0)

    # Hitung total stock per barcode
    df['sumQty'] = pd.to_numeric(df['sumQty'], errors='coerce')
    df['sales'] = pd.to_numeric(df['sales'], errors='coerce')
    df['sumQty'] = df['sumQty'].fillna(0)
    df['sales'] = df['sales'].fillna(0)
    df['stockAwal'] = df['sumQty'] + df['sales']
    df = df.reset_index(drop=True)
    return df

def proc_add_retur(df, df_retur):
    if not df_retur.empty:
        # Merge df with df_retur on 'fv_barcode' and 'fn_whconsiid'
        df = pd.merge(df, df_retur[['fv_barcode', 'retur', 'fn_whconsiid']], on=['fv_barcode', 'fn_whconsiid'], how='left')
        df['retur'] = df['retur'].fillna(0)
    else:
        df['retur'] = 0
    return df

def proc_add_mutasi(df, df_mutasi):
    if not df_mutasi.empty:
        # Merge df with df_mutasi on 'fv_barcode' and 'fn_whconsiid'
        df = pd.merge(df, df_mutasi[['fv_barcode', 'Mutasi', 'fn_whconsiid']], on=['fv_barcode', 'fn_whconsiid'], how='left')
        df['Mutasi'] = df['Mutasi'].fillna(0)
    else:
        df['Mutasi'] = 0
    return df

def proc_add_obdo(df, df_obdo):
    if not df_obdo.empty:
        df = pd.merge(df, df_obdo, on=['fv_barcode','fn_whconsiid'], how='left')

        # Isi nilai NaN di kolom retur dengan 0
        df['qty_ob'] = df['qty_ob'].fillna(0)
        df['qty_do'] = df['qty_do'].fillna(0)

        df['fd_ob'] = pd.to_datetime(df['fd_ob'])
        df['fd_do'] = pd.to_datetime(df['fd_do'])

        # Format '1st DO' to 'YYYY-MM-DD'
        df['fd_ob'] = df['fd_ob'].dt.strftime('%Y-%m-%d')
        df['fd_do'] = df['fd_do'].dt.strftime('%Y-%m-%d')

        # Fill NaN values in stock_on_1st_do with 0 (if no stock data on 1st DO)
        df['fd_ob'] = df['fd_ob'].fillna('-')
        df['fd_do'] = df['fd_do'].fillna('-')
    else:
        df['qty_ob'] = 0
        df['qty_do'] = 0
        df['fd_ob'] = '-'
        df['fd_do'] = '-'
    return df

def proc_add_ostdo(df, df_ostdo, col):
    if not df_ostdo.empty:
        # merge with retur
        
        df = pd.merge(df, df_ostdo, on=[f'{col}','fn_whconsiid'], how='outer')
        df['OstDO'] = df['OstDO'].fillna(0).astype(int)
        # Isi nilai NaN di kolom retur dengan 0
        return df
    else:
        df['OstDO'] = 0
        if col == 'fv_artsizecode':
            df['fv_barcode'] = df.empty
            df['fv_barcode_x'] = df.empty
            df['fv_barcode_y'] = df.empty
        return df

def proc_add_ostdm(df, df_ostdm, col):
    if not df_ostdm.empty:
        # merge with retur

        df = pd.merge(df, df_ostdm, on=[f'{col}','fn_whconsiid'], how='outer')

        # Isi nilai NaN di kolom retur dengan 0
        df['OstDM'] = df['OstDM'].fillna(0).astype(int)
        return df
    else:
        df['OstDM'] = 0
        if col == 'fv_artsizecode':
            df['fv_barcode'] = df.empty
            df['fv_barcode_x'] = df.empty
            df['fv_barcode_y'] = df.empty
        return df

def proc_add_stok_future(df):
    # print(df.info())
    df['stockfuture'] = df['sumQty'] + df['OstDO'] + df['OstDM']
    return df

def proc_selected_col_detail(df):
    df['fv_artsizecode']= df.empty
    df['index']=df.empty
    df=df[['index','fv_toko',
        'fv_barcode',
        'fv_catname',
        'fv_brandname',
        'fv_configname',
        'fv_namemark',
        'fm_price',
        'fv_divname',
        'fv_rating',
        '1st DO',
        'last_receive',
        'aging',
        'fv_colorname',
        'stock_on_1st_do',
        '25',
        '26',
        '27',
        '28',
        '29',
        '30',
        '31',
        '32',
        '33',
        '34',
        '35',
        '36',
        '37',
        '38',
        '39',
        '40',
        '42',
        '44',
        'ALLSIZE',
        'FS',
        'XS',
        'SS',
        'S',
        'M',
        'L',
        'XL',
        'XXL',
        'XXXL',
        'status',
        'sumQty',
        'Ornal',
        'artikel',
        'sales',
        'fc_code',
        'stockAwal',
        'retur',
        'Mutasi',
        'OstDO',
        'fv_artsizecode',
        'fd_ob',
        'qty_ob',
        'fd_do',
        'qty_do',
        'OstDM',
        'stockfuture',
        'fv_status']]
    return df

def proc_selected_col_excel_rename(df):
    df['fv_artsizecode']= df.empty
    df['Sales Thru %'] = (df['sales'] / df['stockAwal'] * 100).round(2)
    df=df[['fv_barcode',
           'fv_toko',
        'fv_catname',
        'fv_brandname',
        'fv_divname',
        'fv_configname',
        'fv_namemark',
        'fm_price',
        'fv_rating',
        'fv_colorname',
        'status',
        'fv_status',
        'Ornal',
        '1st DO',
        'stock_on_1st_do',
        'last_receive',
        'aging',
        'stockAwal',
        'sales',
        'Sales Thru %',
        'sumQty',
        'stockfuture',
        'OstDO',
        'OstDM',
        'retur',
        'Mutasi',
        'qty_ob',
        'qty_do',
        'fd_ob',
        'fd_do',
        '25',
        '26',
        '27',
        '28',
        '29',
        '30',
        '31',
        '32',
        '33',
        '34',
        '35',
        '36',
        '37',
        '38',
        '39',
        '40',
        '42',
        '44',
        'ALLSIZE',
        'FS',
        'XS',
        'SS',
        'S',
        'M',
        'L',
        'XL',
        'XXL',
        'XXXL',
        'sumQty',]]
    df=df.rename(columns={
        'fv_barcode': 'Artikel',
        'fv_toko': 'Toko',
        'fv_catname': 'Kategori',
        'fv_brandname': 'Brand',
        'fv_divname': 'Divisi',
        'fv_configname': 'TOP / BOT',
        'fv_namemark': 'Acara',
        'fm_price': 'Gross',
        'fv_rating': 'Rating',
        'fv_colorname': 'Color',
        'status': 'Keterangan',
        'fv_status': 'Status Produksi',
        'Ornal': 'Obral / Normal',
        '1st DO': '1st DO',
        'stock_on_1st_do': 'Qty 1st DO',
        'last_receive': 'Last Receive',
        'aging': 'Aging (hari)',
        'stockAwal': 'Stock Awal',
        'sales': 'Total Sales',
        'Sales Thru %': ' Sales Thru (%)',
        }).sort_values(by='Toko', ascending=True)
    return df

def proc_selected_col_mobile_view(df):
    df=df[['fv_artsizecode','fv_sizename','sumQty','stockfuture','fv_divname','fv_namemark',
        'fm_price','fv_fitname']]
    return df

def proc_selected_col_stok_retur(df):
    df['SalesThru'] = (df['sales'] / df['stockAwal'] * 100).round(2)
    df['index']=df.empty
    df=df[['index','fv_toko',
        'fv_barcode',
        'fv_configname',
        'fv_namemark',
        'aging',
        '25',
        '26',
        '27',
        '28',
        '29',
        '30',
        '31',
        '32',
        '33',
        '34',
        '35',
        '36',
        '37',
        '38',
        '39',
        '40',
        '42',
        '44',
        'ALLSIZE',
        'FS',
        'XS',
        'SS',
        'S',
        'M',
        'L',
        'XL',
        'XXL',
        'XXXL',
        'status',
        'sumQty',
        'sales',
        'fc_code',
        'retur',
        'Mutasi',
        'stockAwal',
        'SalesThru',
        # 'stockfuture',
        # 'fv_status',
        'fv_divname',
        'fv_catname',]]
    return df

def proc_add_cus_art(df,input_param,type_table,col):
    store_list = input_param['store_list']
    if store_list:
        placeholders = ", ".join([f"'{s}'" for s in store_list])
    df_cus = df__customers_pb(placeholders,"concat(fc_code, ' - ', fv_toko) fv_toko, fn_whconsiid, fc_code")
    if col == 'fv_artsizecode':
        df_art= df_artikel_pb_('fv_barcode,fv_catname,fv_brandname,fv_divname,fv_colorname,fv_rating,fv_configname,fv_namemark,fm_price,fv_sizename,fv_artsizecode,fv_fitname')
        df = pd.merge(df, df_art, on='fv_artsizecode', how='left')
    else:
        df_art= df_artikel_pb_('fv_barcode,fv_catname,fv_brandname,fv_divname,fv_colorname,fv_rating,fv_configname,fv_namemark,fm_price,fv_status').drop_duplicates('fv_barcode')
        df = pd.merge(df, df_art, on='fv_barcode', how='left')
    df = pd.merge(df, df_cus, on='fn_whconsiid', how='left')
    if type_table == 'mobile_view':
        df['fv_sizename_x']=df['fv_sizename_x'].fillna(df['fv_sizename_y'])
        df=df.rename(columns={'fv_sizename_x': 'fv_sizename'})
        return df
    df['artikel']= df['fv_barcode']
    df['fv_artsizecode']= df.empty
    return df

def proc_add_summary(df):
    df_artikel=df_artikel_pb_('fv_barcode,fv_catname').drop_duplicates(subset='fv_barcode')
    df=pd.merge(df, df_artikel, on='fv_barcode', how='left')
    df=df[['fv_barcode','fv_catname','stockAwal','sales','sumQty','status','aging']]
    # Grouping dan agregasi
    summary = df.groupby('fv_catname').agg({
        'sumQty': 'sum',
        'stockAwal': 'sum',
        'sales': 'sum'
    }).reset_index()
    # Tambahkan kolom jumlah SEHAT & BROKEN
    status_count = df.pivot_table(index='fv_catname', columns='status', aggfunc='size', fill_value=0)

    # Hitung aging > 12 per kategori
    df_aging = df[df['aging'] > 12]
    aging_count = df_aging.groupby('fv_catname').size().rename('aging_gt_12')
    df_cart=df.groupby('fv_catname').size().rename('cart')
    grouped_qty = (df.groupby(['fv_catname', 'status'])['sumQty'].sum()
                .unstack(fill_value=0)
                .rename(columns={'BROKEN':'broken pcs','SEHAT':'sehat pcs'}))
    # print(df['status'].value_counts())
    # Gabungkan semuanya
    final_df = summary.join(status_count, on='fv_catname'
                            ).join(df_cart, on='fv_catname'
                                    ).join(aging_count, on='fv_catname'
                                            ).join(grouped_qty, on='fv_catname')
    final_df = final_df.fillna(0).astype({'aging_gt_12': 'int'})  # Pastikan integer
    final_df['sales_contrib'] = ((final_df['sales'] / final_df['sales'].sum())*100).round(2)
    final_df['sales_thru'] = (final_df['sales'] / final_df['stockAwal'] * 100).round(2)
    final_df['total pcs'] = final_df['sumQty']
    final_df=final_df[['fv_catname','stockAwal','sales','sumQty','sales_thru','sales_contrib',
                        'BROKEN','SEHAT','cart','broken pcs','sehat pcs','total pcs','aging_gt_12']]
    final_df=final_df.sort_values('sales_contrib',ascending=False)
    return final_df

def proc_cleaning_data_mobile_view(df):
    # print(df.info())
    df['fv_barcode'] = df['fv_barcode'].fillna(df['fv_barcode_x'].combine_first(df['fv_barcode_y']))
    # print(df.info())
    df[['stocks','OstDO','OstDM']]= df[['stocks','OstDO','OstDM']].fillna(0).astype(int)
    df=df.rename(columns={'stocks':'sumQty'})
    df= df.drop(columns=['fv_barcode_x', 'fv_barcode_y'], errors='ignore')
    return df