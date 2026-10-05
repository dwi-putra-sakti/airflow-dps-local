"""
src_opt.config.settings
Centralized configuration manager for Airflow optimized pipeline.
Safely loads environment variables without printing noise during module imports.
"""
import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv


def _load_environment():
    """Load .env quietly without stdout pollution during DAG parsing."""
    env_paths = [
        Path("/opt/airflow/.env"),
        Path(os.getcwd()) / ".env",
        Path(__file__).resolve().parents[2] / ".env"
    ]
    for path in env_paths:
        if path.is_file():
            load_dotenv(dotenv_path=str(path), override=False)
            return str(path)
    
    # Fallback to find_dotenv if standard locations don't match
    found = find_dotenv()
    if found:
        load_dotenv(found, override=False)
        return found
    return None


# Execute once quietly upon module load
_ENV_FILE_PATH = _load_environment()


class Settings:
    """Read configuration values from environment with sensible fallbacks."""
    
    # Environment info
    ENV_PATH = _ENV_FILE_PATH
    
    # Database configuration
    DB_TYPE = os.getenv("DB_TYPE", "mysql+pymysql")
    DB_USER = os.getenv("DB_USER", "dataanalyst")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_HOST1 = os.getenv("DB_HOST1", "36.93.148.91")   # Sync / Read DB
    DB_HOST2 = os.getenv("DB_HOST2", "172.16.11.9")    # Local / Default DB
    DB_HOST3 = os.getenv("DB_HOST3", "36.93.148.91")   # Real-Time / Write DB
    DB_PORT = int(os.getenv("DB_PORT", "3307"))
    
    DB_SYNC = os.getenv("DB_SYNC", "gis_db1")
    DB_LOCAL = os.getenv("DB_LOCAL", "gis_local")
    DB_RT = os.getenv("DB_RT", "gis_db1")
    
    # Connection Pool Settings
    POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
    MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "10"))
    POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "3600"))
    POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
    
    # Storage / SMB Share configuration
    SMB_PATH = os.getenv("SMB_PATH", "/opt/share")
    
    # Timezone
    TIMEZONE = os.getenv("AIRFLOW_TIMEZONE", "Asia/Jakarta")

    @classmethod
    def get_conn_uri(cls, stat: str = "read") -> str:
        """Construct SQLAlchemy connection URI based on role ('read', 'write', 'local')."""
        if stat == "read":
            host = cls.DB_HOST1
            dbname = cls.DB_SYNC
        elif stat == "write":
            host = cls.DB_HOST3
            dbname = cls.DB_RT
        else:
            host = cls.DB_HOST2
            dbname = cls.DB_LOCAL
            
        return f"{cls.DB_TYPE}://{cls.DB_USER}:{cls.DB_PASSWORD}@{host}:{cls.DB_PORT}/{dbname}"


settings = Settings()
