import os
from sqlalchemy import create_engine
from dotenv import load_dotenv, find_dotenv
from contextlib import contextmanager
# Load env variables
# --- Load .env secara fleksibel ---
def load_env_safely():
    env_path = find_dotenv('/opt/airflow/.env') or find_dotenv()
    if env_path:
        load_dotenv(env_path)
        print(f"✅ .env loaded from: {env_path}")
    else:
        print("⚠️ .env file not found!")

# Panggil saat modul diimport
load_env_safely()
# load_dotenv(dotenv_path='/opt/airflow/.env')
# Ambil variabel dari environment
DB_TYPE = os.getenv("DB_TYPE")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST1 = os.getenv("DB_HOST1")
DB_HOST2 = os.getenv("DB_HOST2")
DB_HOST3 = os.getenv("DB_HOST3")
DB_PORT = os.getenv("DB_PORT")
DB_SYNC = os.getenv("DB_SYNC")
DB_LOCAL = os.getenv("DB_LOCAL") 
DB_RT= os.getenv("DB_RT")
print(DB_HOST1)
@contextmanager
def get_engine(stat):
    """Context manager untuk membuka dan menutup SQLAlchemy engine."""
    if stat == 'read':
        engine = create_engine(f'{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST1}:{DB_PORT}/{DB_SYNC}')
    elif stat == 'write':
        engine = create_engine(f'{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST3}:{DB_PORT}/{DB_RT}')
    else:
        engine = create_engine(f'{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST2}:{DB_PORT}/{DB_LOCAL}')

    try:
        # Mengembalikan engine untuk digunakan dalam blok with
        yield engine
    finally:
        # Menutup koneksi pool engine untuk membersihkan sumber daya
        engine.dispose()
def get_db(stat):
    if stat == 'read':
        db = DB_SYNC
    elif stat == 'write':
        db = DB_RT
    else:
        db = DB_LOCAL
    return db


# import os
# from sqlalchemy import create_engine
# from dotenv import load_dotenv, find_dotenv
# from contextlib import contextmanager

# # --- Load .env secara fleksibel ---
# def load_env_safely():
#     env_path = find_dotenv('/opt/airflow/.env') or find_dotenv()
#     if env_path:
#         load_dotenv(env_path)
#         print(f"✅ .env loaded from: {env_path}")
#     else:
#         print("⚠️ .env file not found!")

# # Panggil saat modul diimport
# load_env_safely()

# # --- Ambil variabel dari environment ---
# DB_TYPE   = os.getenv("DB_TYPE")
# DB_USER   = os.getenv("DB_USER")
# DB_PASS   = os.getenv("DB_PASSWORD")
# DB_HOST   = os.getenv("DB_HOST")
# DB_HOST2  = os.getenv("DB_HOST2", DB_HOST)
# DB_HOST3  = os.getenv("DB_HOST3", DB_HOST)
# DB_PORT   = os.getenv("DB_PORT", "3306")
# DB_SYNC   = os.getenv("DB_SYNC")
# DB_LOCAL  = os.getenv("DB_LOCAL")
# DB_RT     = os.getenv("DB_RT")

# # --- Fungsi koneksi utama ---
# @contextmanager
# def get_engine(stat):
#     """Context manager untuk membuka dan menutup SQLAlchemy engine."""
    
#     # Validasi environment
#     if not DB_TYPE:
#         raise ValueError("❌ DB_TYPE tidak terdefinisi — pastikan .env sudah benar dan berisi DB_TYPE=mysql+pymysql")
    
#     # Pilih koneksi sesuai mode
#     if stat == 'read':
#         print("DB_TYPE:", DB_TYPE)
#         print("DB_USER:", DB_USER)
#         print("DB_PASSWORD:", DB_PASSWORD)
#         print("DB_HOST:", DB_HOST)
#         print("DB_PORT:", DB_PORT)
#         print("DB_SYNC:", DB_SYNC)
#         print("DB_RT:", DB_RT)
#         print("DB_LOCAL:", DB_LOCAL)
#         db_name, db_host = DB_SYNC, DB_HOST
#     elif stat == 'write':
#         db_name, db_host = DB_RT, DB_HOST3
#     else:
#         db_name, db_host = DB_LOCAL, DB_HOST2
    
#     # Pastikan db_name tidak None
#     if not db_name:
#         raise ValueError(f"❌ Nama database untuk mode '{stat}' belum terisi di .env")
    
#     # Buat string koneksi
#     conn_str = f"{DB_TYPE}://{DB_USER}:{DB_PASS}@{db_host}:{DB_PORT}/{db_name}"
#     print(f"🔗 Connecting to: {conn_str.replace(DB_PASS, '***')}")

#     engine = create_engine(conn_str)

#     try:
#         yield engine
#     finally:
#         engine.dispose()
#         print("🧹 Engine disposed.")

# # --- Fungsi bantu untuk mendapatkan nama DB saja ---
# def get_db(stat):
#     if stat == 'read':
#         return DB_SYNC
#     elif stat == 'write':
#         return DB_RT
#     else:
#         return DB_LOCAL

