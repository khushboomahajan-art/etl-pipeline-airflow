import snowflake.connector
import pandas as pd
from loguru import logger
from datetime import datetime
import uuid
import os
from dotenv import load_dotenv

load_dotenv()


def get_snowflake_connection():
    conn = snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        database  = os.getenv("SNOWFLAKE_DATABASE"),
        schema    = os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        role      = os.getenv("SNOWFLAKE_ROLE")
    )
    logger.info("Snowflake connection established")
    return conn


def load_dataframe_to_snowflake(df: pd.DataFrame, table_name: str, conn) -> int:
    from snowflake.connector.pandas_tools import write_pandas
    logger.info(f"Loading {len(df)} rows into {table_name}")
    df = df.copy()

    # Fix 1: Reset index to RangeIndex
    df = df.reset_index(drop=True)

    # Fix 2: Uppercase all column names for Snowflake
    df.columns = [col.upper() for col in df.columns]

    # Fix 3: Convert datetime columns to string
    for col in df.columns:
        if df[col].dtype == "datetime64[ns]":
            df[col] = df[col].astype(str)
        if "DATE" in col and df[col].dtype == "object":
            df[col] = df[col].astype(str)

    logger.info(f"Columns being loaded: {list(df.columns)}")

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name.upper(),
        overwrite=False,
        auto_create_table=False,
        quote_identifiers=False  # Fix 4: prevents quoting column names
    )
    if success:
        logger.info(f"Loaded {nrows} rows into {table_name}")
    else:
        logger.error(f"Failed to load into {table_name}")
    return nrows


def log_pipeline_run(conn, pipeline_name, status,
                     rows_extracted, rows_transformed,
                     rows_loaded, started_at, error_message=None):
    run_id = str(uuid.uuid4())
    completed_at = datetime.now()
    duration = int((completed_at - started_at).total_seconds())
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO ETL_PIPELINE_LOG (
            run_id, pipeline_name, run_date, status,
            rows_extracted, rows_transformed, rows_loaded,
            error_message, started_at, completed_at, duration_secs
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        run_id, pipeline_name,
        completed_at.date().isoformat(),
        status, rows_extracted, rows_transformed,
        rows_loaded, error_message,
        started_at.isoformat(),
        completed_at.isoformat(), duration
    ))
    cursor.close()
    logger.info(f"Run logged: {run_id} | {status} | {duration}s")


def run_full_load(raw_df, analytics_df, validation, started_at):
    conn = get_snowflake_connection()
    rows_loaded = 0
    status = "SUCCESS"
    error_msg = None
    try:
        load_dataframe_to_snowflake(raw_df, "RAW_STOCK_PRICES", conn)
        rows_loaded = load_dataframe_to_snowflake(
            analytics_df, "STOCK_ANALYTICS", conn
        )
        logger.info("Full load completed successfully")
    except Exception as e:
        status = "FAILED"
        error_msg = str(e)
        logger.error(f"Load failed: {e}")
        raise
    finally:
        log_pipeline_run(
            conn=conn,
            pipeline_name="financial_stock_etl",
            status=status,
            rows_extracted=validation.get("total_rows", 0),
            rows_transformed=len(analytics_df),
            rows_loaded=rows_loaded,
            started_at=started_at,
            error_message=error_msg
        )
        conn.close()
