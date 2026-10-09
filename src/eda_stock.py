"""
Script Analisa & EDA Parquet Stock 2026 (Bulan 01)
Dapat dijalankan langsung via terminal: python src/eda_stock.py
Atau diimpor di notebook:
    from eda_stock import run_full_eda, load_stock_df
"""

import os
import pandas as pd
import numpy as np

DEFAULT_BASE_DIR = r"\\192.168.1.254\Share\_From Valdi\PB DB\stock\tahun=2026\bulan=01"

def load_stock_df(target="bz", base_dir=DEFAULT_BASE_DIR):
    """
    Memuat DataFrame stock dengan aman untuk menghindari duplikasi folder.
    
    Parameters:
    - target: 
        'bz'   -> membaca 'stock_bz.parquet' (Hanya Bazaar / FO, 11 toko)
        'dept' -> membaca 'stock_dept.parquet' (Hanya Dept Store, 281 toko)
        'all'  -> membaca 'stock.parquet' (Total gabungan BZ + Dept, 292 toko)
        'dir'  -> membaca seluruh folder langsung (PERINGATAN: menduplikasi data!)
    """
    if target == "bz":
        filepath = os.path.join(base_dir, "stock_bz.parquet")
    elif target == "dept":
        filepath = os.path.join(base_dir, "stock_dept.parquet")
    elif target == "all":
        filepath = os.path.join(base_dir, "stock.parquet")
    elif target == "dir":
        filepath = base_dir
    else:
        raise ValueError(f"Target tidak dikenal: {target}. Gunakan 'bz', 'dept', 'all', atau 'dir'.")
    
    print(f"[LOAD] Membaca data dari: {filepath}")
    df = pd.read_parquet(filepath)
    print(f"[LOAD] Selesai. Shape: {df.shape[0]:,} baris x {df.shape[1]} kolom")
    return df

def run_full_eda(df, label="Stock Parquet"):
    """
    Menjalankan rangkaian EDA komprehensif pada DataFrame stock.
    """
    col_stock = [c for c in df.columns if "stock" in c][0] if any("stock" in c for c in df.columns) else "stock jan_26"
    
    print("=" * 70)
    print(f" HASIL ANALISA EDA: {label.upper()}")
    print("=" * 70)
    
    # 1. Profiling Dasar & Memory
    mem_mb = df.memory_usage(deep=True).sum() / (1024 ** 2)
    print("\n--- 1. PROFILING DASAR ---")
    print(f"Total Baris         : {len(df):,}")
    print(f"Total Kolom         : {df.shape[1]} ({', '.join(df.columns)})")
    print(f"Estimasi Memory     : {mem_mb:.2f} MB")
    
    # 2. Kelengkapan & Missing Values
    print("\n--- 2. DATA QUALITY & MISSING VALUES ---")
    dq = pd.DataFrame({
        'Tipe Data': df.dtypes.astype(str),
        'Null Count': df.isnull().sum(),
        'Null (%)': (df.isnull().sum() / len(df) * 100).round(2),
        'Unique Count': df.nunique()
    })
    print(dq.to_string())
    
    # 3. Pemeriksaan Duplikasi
    full_dup = df.duplicated().sum()
    key_dup = df.duplicated(subset=['fn_whconsiid', 'fv_artsizecode']).sum()
    print("\n--- 3. PEMERIKSAAN DUPLIKASI ---")
    print(f"Duplikasi Baris Penuh               : {full_dup:,} ({full_dup/len(df)*100:.2f}%)")
    print(f"Duplikasi Key (toko + artikel)      : {key_dup:,} ({key_dup/len(df)*100:.2f}%)")
    if full_dup > 0:
        print("  [PERINGATAN] Terdeteksi duplikasi! Kemungkinan file dibaca dari direktori yang berisi banyak parquet.")

    # 4. Analisa Statistik Kolom Stock
    s_col = df[col_stock]
    print(f"\n--- 4. ANALISA NILAI '{col_stock}' ---")
    print(f"Total Qty Stock     : {s_col.sum():,.0f}")
    print(f"Rata-rata (Mean)    : {s_col.mean():.4f}")
    print(f"Standar Deviasi     : {s_col.std():.4f}")
    print(f"Nilai Minimum       : {s_col.min()}")
    print(f"Nilai Maksimum      : {s_col.max()}")
    
    # Distribusi Persentil
    percentiles = [0.01, 0.05, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.999]
    pct_series = s_col.quantile(percentiles)
    print("\nPersentil Nilai Stock:")
    for p, val in pct_series.items():
        print(f"  P{int(p*1000)/10:>4.1f}% : {val:>6.1f}")
        
    # Segmentasi Stock
    pos_count = (s_col > 0).sum()
    zero_count = (s_col == 0).sum()
    neg_count = (s_col < 0).sum()
    print("\nSegmentasi Nilai Stock:")
    print(f"  - Positif (> 0)  : {pos_count:>10,} baris ({pos_count/len(df)*100:>5.2f}%) | Total Qty: {s_col[s_col > 0].sum():>12,.0f}")
    print(f"  - Nol     (= 0)  : {zero_count:>10,} baris ({zero_count/len(df)*100:>5.2f}%) | Total Qty: 0")
    print(f"  - Negatif (< 0)  : {neg_count:>10,} baris ({neg_count/len(df)*100:>5.2f}%) | Total Qty: {s_col[s_col < 0].sum():>12,.0f}")
    
    # 5. Analisa Anomali Stok Negatif
    if neg_count > 0:
        print("\n--- 5. DETAIL ANOMALI STOK NEGATIF ---")
        df_neg = df[s_col < 0]
        neg_by_wh = df_neg.groupby('fn_whconsiid').agg(
            baris_neg=('fn_whconsiid', 'count'),
            sum_neg=(col_stock, 'sum'),
            min_val=(col_stock, 'min')
        ).sort_values(by='sum_neg').head(5)
        print("Top Toko dengan Stok Negatif Terbanyak:")
        print(neg_by_wh.to_string())
        
        batal_count = df_neg['fv_artsizecode'].str.contains('BATAL', na=False).sum()
        print(f"Artikel dengan flag '-BATAL': {batal_count} baris")
        print("\nSampel 3 Data Stok Negatif:")
        print(df_neg[['fn_whconsiid', 'fv_artsizecode', col_stock]].head(3).to_string(index=False))

    # 6. Analisa Toko / Warehouse
    print(f"\n--- 6. PROFIL TOKO / WAREHOUSE (fn_whconsiid) ---")
    n_wh = df['fn_whconsiid'].nunique()
    print(f"Jumlah Toko Unik    : {n_wh}")
    wh_agg = df.groupby('fn_whconsiid').agg(
        total_sku=('fv_artsizecode', 'count'),
        total_stock=(col_stock, 'sum'),
        sku_positif=(col_stock, lambda x: (x > 0).sum()),
        sku_nol=(col_stock, lambda x: (x == 0).sum()),
        sku_negatif=(col_stock, lambda x: (x < 0).sum()),
        max_stock=(col_stock, 'max'),
        min_stock=(col_stock, 'min')
    ).sort_values(by='total_stock', ascending=False)
    
    print("\nTop 5 Toko Terbesar Berdasarkan Stock:")
    print(wh_agg.head(5).to_string())

    # 7. Analisa Artikel / SKU
    print(f"\n--- 7. PROFIL ARTIKEL (fv_artsizecode) ---")
    n_art = df['fv_artsizecode'].nunique()
    print(f"Jumlah Artikel Unik : {n_art:,}")
    art_agg = df.groupby('fv_artsizecode')[col_stock].agg(['count', 'sum']).sort_values(by='sum', ascending=False)
    print("\nTop 5 Artikel Terbanyak Berdasarkan Total Stock:")
    print(art_agg.head(5).to_string())
    print("=" * 70)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="EDA Parquet Stock 2026")
    parser.add_argument("--target", choices=["bz", "dept", "all", "dir"], default="bz",
                        help="Target dataset yang dianalisa (default: bz)")
    args = parser.parse_args()
    
    df = load_stock_df(target=args.target)
    run_full_eda(df, label=f"Stock Parquet ({args.target.upper()})")
