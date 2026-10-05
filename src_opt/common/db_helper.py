"""
src_opt.common.db_helper
Database helper utilities for efficient DataFrame persistence, type mapping, and indexing.
"""
from typing import Optional, List, Dict, Any
import pandas as pd
from sqlalchemy import Integer, String, CHAR, DateTime, Date, Text, Float, text
from sqlalchemy.engine import Engine

from src_opt.config.database import get_engine, get_pooled_engine
from src_opt.config.logger import get_pipeline_logger

logger = get_pipeline_logger("src_opt.common.db_helper")


def define_sql_types(df: pd.DataFrame, return_types_only: str = 'yes'):
    """
    Map DataFrame dtypes to SQLAlchemy column types and adjust column prefixes.
    """
    sql_types = {}
    renamed_columns = {}
    
    for col in df.columns:
        if pd.api.types.is_integer_dtype(df[col]):
            new_col_name = f'fn_{col}' if not col.startswith('fn_') else col
            sql_types[new_col_name] = Integer
        elif pd.api.types.is_string_dtype(df[col]):
            max_len = df[col].astype(str).str.len().max()
            if max_len > 255:
                new_col_name = f'fv_{col}' if not col.startswith('fv_') else col
                sql_types[new_col_name] = Text
            else:
                new_col_name = f'fv_{col}' if not col.startswith('fv_') else col
                sql_types[new_col_name] = String(max(50, min(int(max_len), 255)))
        elif col.startswith('fc'):
            new_col_name = f'fc_{col}' if not col.startswith('fc_') else col
            sql_types[new_col_name] = String(50)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            new_col_name = f'fd_{col}' if not col.startswith('fd_') else col
            sql_types[new_col_name] = Date
        else:
            new_col_name = f'fv_{col}' if not col.startswith('fv_') else col
            sql_types[new_col_name] = String(50)
        
        renamed_columns[col] = new_col_name
        
    df.rename(columns=renamed_columns, inplace=True)
    if return_types_only == 'yes':
        return sql_types
    return df


def safe_to_sql(
    df: pd.DataFrame,
    table_name: str,
    stat: str = 'imp',
    if_exists: str = 'replace',
    chunksize: int = 5000
) -> None:
    """
    Save DataFrame to database using connection pool in manageable chunks.
    """
    if df is None or df.empty:
        logger.warning(f"DataFrame for table '{table_name}' is empty. Skipping insertion.")
        return
        
    engine = get_pooled_engine(stat)
    sql_types = define_sql_types(df, 'yes')
    
    total_rows = len(df)
    logger.info(f"Writing {total_rows} rows to table '{table_name}' ({if_exists}) on '{stat}'...")
    
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists=if_exists,
        index=False,
        dtype=sql_types,
        chunksize=chunksize
    )
    logger.info(f"Successfully saved {total_rows} rows to '{table_name}'.")


def add_index_and_foreign_keys(
    stat: str,
    table_name: str,
    index_cols: Optional[List[str]] = None,
    foreign_keys: Optional[List[Dict[str, str]]] = None
) -> None:
    """
    Add indexes and foreign key constraints cleanly.
    """
    engine = get_pooled_engine(stat)
    with engine.connect() as conn:
        if index_cols:
            for col in index_cols:
                idx_name = f"idx_{table_name}_{col}"
                try:
                    conn.execute(text(f"CREATE INDEX `{idx_name}` ON `{table_name}` (`{col}`);"))
                    logger.info(f"Created index {idx_name} on {table_name}({col})")
                except Exception as e:
                    logger.debug(f"Index {idx_name} could not be created or already exists: {e}")

        if foreign_keys:
            for fk in foreign_keys:
                c_name = fk.get("constraint_name", f"fk_{table_name}_{fk['column']}")
                try:
                    sql = f"""
                    ALTER TABLE `{table_name}`
                    ADD CONSTRAINT `{c_name}`
                    FOREIGN KEY (`{fk['column']}`) REFERENCES `{fk['ref_table']}` (`{fk['ref_column']}`);
                    """
                    conn.execute(text(sql))
                    logger.info(f"Added FK {c_name} on {table_name}")
                except Exception as e:
                    logger.debug(f"FK {c_name} could not be created or already exists: {e}")


def bulk_upsert(
    df: pd.DataFrame,
    table_name: str,
    primary_key: str = "fn_whconsiid",
    stat: str = "imp",
    chunksize: int = 2000
) -> None:
    """
    Perform native MySQL bulk upsert (INSERT ... ON DUPLICATE KEY UPDATE).
    Upserts all columns simultaneously in chunks within a transactional session,
    reducing execution time from tens of minutes to seconds.
    """
    from sqlalchemy import MetaData
    from sqlalchemy.dialects.mysql import insert as mysql_insert
    from sqlalchemy.orm import sessionmaker

    if df is None or df.empty:
        logger.warning(f"DataFrame for upsert into '{table_name}' is empty. Skipping.")
        return

    engine = get_pooled_engine(stat)
    metadata = MetaData()
    metadata.reflect(bind=engine, only=[table_name])

    if table_name not in metadata.tables:
        raise ValueError(f"Table '{table_name}' not found in database for mode '{stat}'!")

    table = metadata.tables[table_name]
    db_cols = set(col.name for col in table.columns)

    # Filter only columns that exist in the target table
    valid_cols = [c for c in df.columns if c in db_cols]
    if not valid_cols:
        raise ValueError(f"No matching columns found between DataFrame and table '{table_name}'")

    clean_df = df[valid_cols].where(pd.notnull(df[valid_cols]), None)
    total_rows = len(clean_df)
    logger.info(f"Bulk upserting {total_rows} rows into '{table_name}' across {len(valid_cols)} columns...")

    # Determine columns to update on duplicate key (all except primary key)
    pk_cols = [primary_key] if isinstance(primary_key, str) else primary_key
    update_cols = [c for c in valid_cols if c not in pk_cols]

    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        for i in range(0, total_rows, chunksize):
            chunk = clean_df.iloc[i : i + chunksize]
            records = chunk.to_dict(orient="records")

            stmt = mysql_insert(table).values(records)
            update_dict = {col: stmt.inserted[col] for col in update_cols}
            upsert_stmt = stmt.on_duplicate_key_update(update_dict)

            session.execute(upsert_stmt)
        session.commit()
        logger.info(f"✔ Successfully bulk upserted {total_rows} rows into '{table_name}'.")
    except Exception as exc:
        session.rollback()
        logger.error(f"❌ Bulk upsert failed for '{table_name}': {exc}", exc_info=True)
        raise
    finally:
        session.close()

