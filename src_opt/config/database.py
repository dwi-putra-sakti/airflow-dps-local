"""
src_opt.config.database
High-performance connection pooling and engine management for MySQL/MariaDB databases.
Replaces repetitive create_engine() calls with pooled, pre-pinged engines.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Generator, Dict
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .settings import settings
from .logger import get_pipeline_logger

logger = get_pipeline_logger("src_opt.database")

# Global cache for persistent connection pools
_ENGINES: Dict[str, Engine] = {}


def get_pooled_engine(stat: str = "read") -> Engine:
    """
    Retrieve or create a cached SQLAlchemy Engine with connection pooling.
    Pool pre-ping ensures dead TCP connections are detected and renewed automatically.
    """
    if stat not in _ENGINES:
        conn_uri = settings.get_conn_uri(stat)
        sanitized_uri = conn_uri.replace(settings.DB_PASSWORD, "******") if settings.DB_PASSWORD else conn_uri
        logger.info(f"Initializing connection pool for '{stat}' -> {sanitized_uri}")
        
        _ENGINES[stat] = create_engine(
            conn_uri,
            pool_size=settings.POOL_SIZE,
            max_overflow=settings.MAX_OVERFLOW,
            pool_recycle=settings.POOL_RECYCLE,
            pool_timeout=settings.POOL_TIMEOUT,
            pool_pre_ping=True,  # Heartbeat ping to prevent 'MySQL server has gone away'
        )
    return _ENGINES[stat]


@contextmanager
def get_engine(stat: str = "read") -> Generator[Engine, None, None]:
    """
    Context manager yielding a pooled Engine.
    Compatible with legacy syntax: `with get_engine('read') as engine: pd.read_sql(q, engine)`
    """
    engine = get_pooled_engine(stat)
    try:
        yield engine
    finally:
        # Note: Do not dispose engine here to allow connection reuse in pool.
        pass


@contextmanager
def get_connection(stat: str = "read"):
    """
    Context manager yielding an active Connection from the pool.
    Recommended for transactional SQL execution.
    """
    engine = get_pooled_engine(stat)
    with engine.connect() as connection:
        yield connection


def get_db(stat: str = "read") -> str:
    """Return database name based on mode ('read', 'write', 'local')."""
    if stat == "read":
        return settings.DB_SYNC
    elif stat == "write":
        return settings.DB_RT
    return settings.DB_LOCAL


def dispose_all_pools():
    """Explicitly clean up all pooled connections if worker process terminates."""
    for stat, engine in _ENGINES.items():
        try:
            engine.dispose()
            logger.info(f"Disposed engine pool for '{stat}'")
        except Exception as e:
            logger.warning(f"Error disposing engine '{stat}': {e}")
    _ENGINES.clear()
