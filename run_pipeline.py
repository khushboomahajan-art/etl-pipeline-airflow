from etl.extract import extract_all_tickers
from etl.transform import transform
from etl.load import run_full_load
from datetime import datetime
from loguru import logger

if __name__ == "__main__":
    started_at = datetime.now()
    logger.info("=== Pipeline Started ===")

    # Step 1: Extract
    logger.info("Step 1: Extracting data...")
    raw_df = extract_all_tickers()

    # Step 2: Transform
    logger.info("Step 2: Transforming data...")
    clean_df, analytics_df, validation = transform(raw_df)

    # Step 3: Load
    logger.info("Step 3: Loading to Snowflake...")
    run_full_load(clean_df, analytics_df, validation, started_at)

    logger.info("=== Pipeline Completed Successfully ===")
