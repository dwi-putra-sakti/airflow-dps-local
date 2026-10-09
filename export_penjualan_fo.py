import os
import sys
import calendar
import pandas as pd
import numpy as np
import openpyxl
from openpyxl.styles import Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# Pastikan folder 'src' masuk ke sys.path agar modul queries dan db_connection terbaca
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(BASE_DIR, 'src')
if os.path.exists(SRC_DIR) and SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

try:
    from modules.queries import df__penju_bz, df__customers_pb_
    from modules.db_connection import get_engine
except ImportError:
    from src.modules.queries import df__penju_bz, df__customers_pb_
    from src.modules.db_connection import get_engine


def get_store_names():
    """Mengambil pemetaan fn_whconsiid ke nama toko (fv_toko)."""
    try:
        df_cus = df__customers_pb_('fn_whconsiid, fv_toko')
        if df_cus is not None and not df_cus.empty:
            return df_cus[['fn_whconsiid', 'fv_toko']].drop_duplicates()
    except Exception:
        pass

    try:
        with get_engine('write') as eng:
            df_cus = pd.read_sql("""
                SELECT a.fn_whconsiid, b.fv_namecus AS fv_toko 
                FROM warehouseconsi_tb a 
                JOIN customer_tb b ON a.fn_cusid = b.fn_cusid
            """, eng)
            return df_cus[['fn_whconsiid', 'fv_toko']].drop_duplicates()
    except Exception as e:
        print(f"[WARN] Gagal mengambil nama toko dari database: {e}")
        return pd.DataFrame(columns=['fn_whconsiid', 'fv_toko'])


def categorize_price(price):
    """
    Kategori rentang harga sesuai template laporan FO:
    - > 300k        -> label: '<300k bp qty'
    - <= 100k       -> label: '>100k bp qty'
    - 100k s/d 200k -> label: '100k-200k bp qty'
    - 200k s/d 300k -> label: '200k-300k bp qty'
    """
    if price > 300000:
        return '<300k bp qty'
    elif price <= 100000:
        return '>100k bp qty'
    elif 100000 < price <= 200000:
        return '100k-200k bp qty'
    elif 200000 < price <= 300000:
        return '200k-300k bp qty'
    return 'other'


def build_fo_summary(df_month):
    """
    Menghasilkan DataFrame summary pivot per toko dan rentang harga BP.
    """
    # Kolom kategori harga sesuai urutan template
    cols_order = ['<300k bp qty', '>100k bp qty', '100k-200k bp qty', '200k-300k bp qty']
    
    # Tambahkan kategori harga
    df_month['price_cat'] = df_month['fn_hargasatuan'].apply(categorize_price)

    # Buat pivot table (baris: fv_toko, kolom: price_cat, value: sum fn_jualpersize)
    pivot = df_month.pivot_table(
        index='fv_toko',
        columns='price_cat',
        values='fn_jualpersize',
        aggfunc='sum'
    ).reindex(columns=cols_order)

    # Sort toko dengan urutan yang rapi
    pivot.reset_index(inplace=True)
    pivot.rename(columns={'fv_toko': 'toko'}, inplace=True)
    
    # Urutkan nama toko
    def sort_key(name):
        # Memastikan FO GRAND CITY MALL - SURABAYA sebelum (GABRIELLE JEANS)
        clean = str(name).replace(' - SURABAYA', ' 1').replace(' (GABRIELLE JEANS)', ' 2')
        return clean

    pivot['sort_key'] = pivot['toko'].apply(sort_key)
    pivot.sort_values(by='sort_key', inplace=True)
    pivot.drop(columns=['sort_key'], inplace=True)

    return pivot, cols_order


def export_penjualan_fo_to_excel(year: int, month: int, output_dir: str = "."):
    """
    Mengambil data penjualan FO, membuat summary sesuai template yang diminta,
    dan menyimpannya dalam format Excel ber-styling rapi.
    """
    nama_bulan = calendar.month_name[month]
    print(f"\n[1/4] Mengambil data penjualan FO tahun {year} dari database...")
    
    # 1. Panggil query dari src/modules/queries.py
    df = df__penju_bz(year)
    
    if df is None or df.empty:
        print(f"[PERINGATAN] Tidak ada data transaksi yang ditemukan untuk tahun {year}.")
        return None

    # 2. Filter bulan berdasarkan kolom tgltrx (%m-%d-%Y)
    print(f"[2/4] Memfilter transaksi untuk bulan {nama_bulan} {year}...")
    df['tgl_parsed'] = pd.to_datetime(df['tgltrx'], format='%m-%d-%Y', errors='coerce')
    df_month = df[df['tgl_parsed'].dt.month == month].copy()
    df_month.drop(columns=['tgl_parsed'], inplace=True)

    if df_month.empty:
        print(f"[PERINGATAN] Tidak ditemukan transaksi untuk bulan {nama_bulan} {year}.")
        return None

    # Gabungkan dengan nama toko
    df_cus = get_store_names()
    df_month = df_month.merge(df_cus, on='fn_whconsiid', how='left')
    df_month['fv_toko'] = df_month['fv_toko'].fillna(df_month['fn_whconsiid'].astype(str))

    # 3. Bentuk Summary Table
    print("[3/4] Membuat Summary Data Penjualan FO...")
    pivot_df, cols_order = build_fo_summary(df_month)

    # 4. Tulis ke file Excel dengan Openpyxl & Styling rapi
    os.makedirs(output_dir, exist_ok=True)
    nama_file = f"Data Penjualan FO {nama_bulan} {year}.xlsx"
    file_path = os.path.join(output_dir, nama_file)
    print(f"[4/4] Menyimpan file Excel ke '{file_path}'...")

    wb = openpyxl.Workbook()
    
    # Sheet 1: Summary FO
    ws_summary = wb.active
    ws_summary.title = f"FO {nama_bulan[:3].lower()} {str(year)[-2:]}"

    # Styles
    font_header = Font(name='Aptos Narrow', size=11, bold=True)
    font_data = Font(name='Aptos Narrow', size=11, bold=False)
    font_total = Font(name='Aptos Narrow', size=11, bold=True)

    align_left = Alignment(horizontal='left', vertical='center')
    align_right = Alignment(horizontal='right', vertical='center')
    align_center = Alignment(horizontal='center', vertical='center')

    border_thin = Border(
        left=Side(style='thin', color='000000'),
        right=Side(style='thin', color='000000'),
        top=Side(style='thin', color='000000'),
        bottom=Side(style='thin', color='000000')
    )
    border_total = Border(
        left=Side(style='thin', color='000000'),
        right=Side(style='thin', color='000000'),
        top=Side(style='medium', color='000000'),
        bottom=Side(style='medium', color='000000')
    )

    # Header Row
    headers = ['toko'] + cols_order
    ws_summary.append(headers)
    for col_idx in range(1, len(headers) + 1):
        cell = ws_summary.cell(row=1, column=col_idx)
        cell.font = font_header
        cell.border = border_thin
        cell.alignment = align_left if col_idx == 1 else align_right

    # Data Rows
    current_row = 2
    for _, row in pivot_df.iterrows():
        row_vals = [row['toko']]
        for col_name in cols_order:
            val = row[col_name]
            # Kosongkan jika NaN / 0 agar persis seperti contoh tampilan
            row_vals.append(int(val) if pd.notna(val) and val > 0 else None)
        
        ws_summary.append(row_vals)
        for col_idx in range(1, len(headers) + 1):
            cell = ws_summary.cell(row=current_row, column=col_idx)
            cell.font = font_data
            cell.border = border_thin
            cell.alignment = align_left if col_idx == 1 else align_right
        current_row += 1

    # Grand Total Row
    last_data_row = current_row - 1
    total_row_vals = ['Grand Total']
    for idx, col_name in enumerate(cols_order):
        col_letter = get_column_letter(idx + 2)
        total_row_vals.append(f"=SUM({col_letter}2:{col_letter}{last_data_row})")
    
    ws_summary.append(total_row_vals)
    for col_idx in range(1, len(headers) + 1):
        cell = ws_summary.cell(row=current_row, column=col_idx)
        cell.font = font_total
        cell.border = border_total
        cell.alignment = align_left if col_idx == 1 else align_right

    # Auto-fit Column Width
    for col in ws_summary.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            val_str = str(cell.value or '')
            if not val_str.startswith('='):
                max_len = max(max_len, len(val_str))
            else:
                max_len = max(max_len, 6)
        ws_summary.column_dimensions[col_letter].width = max(max_len + 3, 12)

    # Sheet 2: Detail Transaksi
    ws_detail = wb.create_sheet(title="Detail Transaksi")
    detail_cols = ['fv_toko', 'fv_nopenjualan', 'tgltrx', 'fv_artsizecode', 
                   'fn_jualpersize', 'fn_hargasatuan', 'total_penjualan2', 
                   'status_artikel', 'desc', 'event']
    available_cols = [c for c in detail_cols if c in df_month.columns]
    
    ws_detail.append(available_cols)
    for cell in ws_detail[1]:
        cell.font = font_header
        cell.border = border_thin
    
    for row in df_month[available_cols].itertuples(index=False):
        ws_detail.append(list(row))
    
    for col in ws_detail.columns:
        col_letter = get_column_letter(col[0].column)
        max_len = max(len(str(cell.value or '')) for cell in col[:100])
        ws_detail.column_dimensions[col_letter].width = min(max(max_len + 2, 10), 40)

    # Simpan workbook
    wb.save(file_path)

    # Tampilkan summary di terminal
    print("\n" + "=" * 65)
    print(f" Summary Data Penjualan FO - Periode {nama_bulan} {year}")
    print("=" * 65)
    
    # Print pivot table preview
    print(pivot_df.to_string(index=False))
    print("-" * 65)
    print(f"File Excel tersimpan di: {os.path.abspath(file_path)}")
    print("Sheet 1: Summary FO (persis sesuai template tabel)")
    print("Sheet 2: Detail Transaksi (data mentah per artikel)")
    print("=" * 65 + "\n")

    return file_path


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        try:
            tahun = int(sys.argv[1])
            bulan = int(sys.argv[2])
        except ValueError:
            print("[ERROR] Parameter tahun dan bulan harus berupa angka!")
            sys.exit(1)
    else:
        print("--- EXPORT SUMMARY DATA PENJUALAN FO KE EXCEL ---")
        try:
            tahun = int(input("Masukkan Tahun (contoh: 2026): ").strip())
            bulan = int(input("Masukkan Bulan (1 - 12): ").strip())
        except (ValueError, KeyboardInterrupt):
            print("\n[BATAL] Input tidak valid atau dibatalkan.")
            sys.exit(1)

    if not (1 <= bulan <= 12):
        print("[ERROR] Bulan harus berada dalam rentang 1 sampai 12!")
        sys.exit(1)

    export_penjualan_fo_to_excel(year=tahun, month=bulan)
