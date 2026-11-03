import os
import pandas as pd

def extract_document_dpl(excel_path=os.path.join("src", "MEETING 2026.xlsx")):
    """
    Mengambil kolom 'No. Document' dan 'No. DPL' dari:
    - Sheet 1: DATA EXPEDITION BILL 2025 (header baris ke-4 / index 3)
    - Sheet 2: DATA EXPEDITION BILL 2026 (header baris ke-1 / index 0)
    
    Menggabungkan keduanya menjadi satu DataFrame dengan 2 kolom.
    """
    print(f"Membaca file: {excel_path}")
    
    # 1. Sheet 2025 (Header berada di baris ke-4)
    df_2025 = pd.read_excel(
        excel_path,
        sheet_name="DATA EXPEDITION BILL 2025",
        header=3,
        usecols=["No. Document", "No. DPL"]
    )
    print(f"-> Sheet 2025: {len(df_2025):,} baris")

    # 2. Sheet 2026 (Header berada di baris ke-1)
    df_2026 = pd.read_excel(
        excel_path,
        sheet_name="DATA EXPEDITION BILL 2026",
        header=0,
        usecols=["No. Document", "No. DPL"]
    )
    print(f"-> Sheet 2026: {len(df_2026):,} baris")

    # 3. Gabungkan kedua DataFrame
    df_combined = pd.concat([df_2025, df_2026], ignore_index=True)

    # Pembersihan spasi
    df_combined["No. Document"] = df_combined["No. Document"].astype(str).str.strip()
    df_combined["No. DPL"] = df_combined["No. DPL"].astype(str).str.strip()
    df_combined["No. DPL"] = df_combined["No. DPL"].replace({"nan": None, "None": None, "": None})

    print(f"-> Total Gabungan: {len(df_combined):,} baris")
    return df_combined

if __name__ == "__main__":
    df = extract_document_dpl()
    print("\n--- 5 Baris Pertama ---")
    print(df.head())
    print("\n--- 5 Baris Terakhir ---")
    print(df.tail())
