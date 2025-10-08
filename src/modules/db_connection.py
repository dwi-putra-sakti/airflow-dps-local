import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from contextlib import contextmanager
# Load env variables
load_dotenv(dotenv_path='/opt/airflow/.env')
# Ambil variabel dari environment
DB_TYPE = os.getenv("DB_TYPE")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_HOST2 = os.getenv("DB_HOST2")
DB_HOST3 = os.getenv("DB_HOST3")
DB_PORT = os.getenv("DB_PORT")
DB_SYNC = os.getenv("DB_SYNC")
DB_LOCAL = os.getenv("DB_LOCAL") 
DB_RT= os.getenv("DB_RT")

@contextmanager
def get_engine(stat):
    """Context manager untuk membuka dan menutup SQLAlchemy engine."""
    if stat == 'read':
        engine = create_engine(f'{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_SYNC}')
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
