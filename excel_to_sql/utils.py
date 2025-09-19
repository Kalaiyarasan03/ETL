import pandas as pd

import logging

logger = logging.getLogger(__name__)
from sqlalchemy import (
    create_engine, Table, Column, Integer, BigInteger, String, Float, MetaData, inspect, Text
)

ALLOWED_EXTS = {'.xlsx', '.xls', '.csv'}

def read_any_tabular(file_obj) -> pd.DataFrame:
    """Read CSV or Excel robustly."""
    try:
        return pd.read_csv(file_obj)
    except Exception:
        try:
            return pd.read_excel(file_obj)
        except Exception:
            raise ValueError("Unsupported file type or corrupted file")

def get_mysql_url() -> str:
    return "mysql+mysqlconnector://root:American123$@192.168.2.77:3306/mfu_db"

def pandas_to_sqlalchemy_type(dtype, sample_values=None):
    """Map pandas dtype to SQLAlchemy type, using BigInteger if needed."""
    s = str(dtype)
    if s.startswith("int"):
        if sample_values is not None:
            max_val = sample_values.max()
            min_val = sample_values.min()
            if max_val > 2147483647 or min_val < -2147483648:
                return BigInteger
        return Integer
    if s.startswith("float"):
        return Float
    if s in {"object", "string"}:
        return Text
    return String(255)

def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Convert numeric columns to integers where appropriate and fill NaNs."""
    int_like_cols = df.select_dtypes(include=['int64', 'float64']).columns
    for col in int_like_cols:
        if pd.api.types.is_float_dtype(df[col]):
            if (df[col].dropna() % 1 == 0).all():
                df[col] = df[col].astype('Int64')
    # Fill NaNs in object columns
    obj_cols = df.select_dtypes(include=['object']).columns
    for col in obj_cols:
        df[col] = df[col].fillna('-')
    # Fill NaNs in numeric columns
    num_cols = df.select_dtypes(include=['Int64', 'float64']).columns
    for col in num_cols:
        df[col] = df[col].fillna(0)
    return df

def upload_excel_dynamic(file_path: str, table_name: str, mysql_url: str = None) -> dict:
    logger.info(f"Reading file: {file_path}")
    df = read_any_tabular(file_path)
    if df.empty:
        logger.error("Uploaded file has no rows.")
        raise ValueError("Uploaded file has no rows.")

    # Normalize column names
    df.columns = [str(c).strip().replace(' ', '_').lower() for c in df.columns]
    logger.info(f"Normalized columns: {df.columns.tolist()}")
    df = sanitize_dataframe(df)

    if not mysql_url:
        mysql_url = get_mysql_url()

    logger.info(f"Connecting to database: {mysql_url}")
    engine = create_engine(mysql_url)
    metadata = MetaData()
    inspector = inspect(engine)

    created = False
    if inspector.has_table(table_name):
        logger.info(f"Table '{table_name}' exists. Checking columns.")
        table = Table(table_name, metadata, autoload_with=engine)
        existing_cols = set(c.name for c in table.columns)

        for col in df.columns:
            if col not in existing_cols:
                dtype = pandas_to_sqlalchemy_type(df[col].dtype, df[col])
                logger.info(f"Adding missing column '{col}' with type {dtype}.")
                with engine.begin() as conn:
                    conn.execute(f'ALTER TABLE {table_name} ADD COLUMN `{col}` {dtype().compile(engine.dialect)}')
    else:
        logger.info(f"Creating new table '{table_name}'.")
        columns = [Column('id', Integer, primary_key=True, autoincrement=True)]
        for col, dtype in df.dtypes.items():
            coltype = pandas_to_sqlalchemy_type(dtype, df[col])
            columns.append(Column(col, coltype, nullable=True))
        Table(table_name, metadata, *columns)
        metadata.create_all(engine)
        created = True

    logger.info(f"Inserting {len(df)} rows into '{table_name}'.")
    with engine.begin() as conn:
        df.to_sql(table_name, con=conn, if_exists='append', index=False, chunksize=1000, method='multi')

    return {
        "table": table_name,
        "created": created,
        "rows_inserted": int(len(df)),
        "columns_count": len(df.columns),
        "columns": list(df.columns),
    }
