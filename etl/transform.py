import pandas as pd
import numpy as np
from loguru import logger
from datetime import datetime


def validate_raw_data(df: pd.DataFrame) -> dict:
    logger.info("Running data quality validation")
    results = {
        "total_rows":       len(df),
        "null_close":       df["close_price"].isnull().sum(),
        "null_dates":       df["price_date"].isnull().sum(),
        "negative_prices":  (df["close_price"] < 0).sum(),
        "duplicate_rows":   df.duplicated(subset=["ticker", "price_date"]).sum(),
        "tickers_found":    df["ticker"].nunique(),
        "passed":           True
    }
    if results["negative_prices"] > 0:
        logger.error(f"Found {results['negative_prices']} negative prices")
        results["passed"] = False
    if results["duplicate_rows"] > 0:
        logger.warning(f"Found {results['duplicate_rows']} duplicate rows")
    logger.info(f"Validation: {'PASSED' if results['passed'] else 'FAILED'}")
    return results


def clean_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning raw data")
    df = df.copy()
    df["price_date"] = pd.to_datetime(df["price_date"])
    df = df.drop_duplicates(subset=["ticker", "price_date"], keep="last")
    df = df.dropna(subset=["close_price", "price_date", "ticker"])
    df = df[df["close_price"] > 0]
    df = df.sort_values(["ticker", "price_date"])
    logger.info(f"Clean data shape: {df.shape}")
    return df


def calculate_analytics(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Calculating analytics metrics")
    df = df.copy()
    df["daily_return_pct"] = df.groupby("ticker")["close_price"].pct_change() * 100
    df["avg_price_7d"] = (
        df.groupby("ticker")["close_price"]
        .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
    )
    df["avg_price_30d"] = (
        df.groupby("ticker")["close_price"]
        .transform(lambda x: x.rolling(window=30, min_periods=1).mean())
    )
    df["price_vs_30d_avg"] = (
        (df["close_price"] - df["avg_price_30d"]) / df["avg_price_30d"] * 100
    )
    df["volatility_7d"] = (
        df.groupby("ticker")["daily_return_pct"]
        .transform(lambda x: x.rolling(window=7, min_periods=1).std())
    )
    df["avg_volume_30d"] = (
        df.groupby("ticker")["volume"]
        .transform(lambda x: x.rolling(window=30, min_periods=1).mean())
    )
    df["volume_vs_avg"] = df["volume"] / df["avg_volume_30d"]
    decimal_cols = [
        "daily_return_pct", "avg_price_7d", "avg_price_30d",
        "price_vs_30d_avg", "volatility_7d", "volume_vs_avg"
    ]
    df[decimal_cols] = df[decimal_cols].round(4)
    df["processed_at"] = datetime.now()
    logger.info(f"Analytics complete: {df.shape}")
    return df


def transform(raw_df: pd.DataFrame):
    logger.info("Starting transformation pipeline")
    validation = validate_raw_data(raw_df)
    clean_df = clean_raw_data(raw_df)
    analytics_df = calculate_analytics(clean_df)
    analytics_final = analytics_df[[
        "ticker", "price_date", "close_price",
        "daily_return_pct", "volatility_7d",
        "avg_price_7d", "avg_price_30d",
        "price_vs_30d_avg", "volume",
        "volume_vs_avg", "processed_at"
    ]]
    logger.info("Transformation complete")
    return clean_df, analytics_final, validation


if __name__ == "__main__":
    import sys
    sys.path.append("..")
    from etl.extract import extract_all_tickers
    raw = extract_all_tickers()
    clean, analytics, validation = transform(raw)
    print("\nValidation Results:")
    for k, v in validation.items():
        print(f"  {k}: {v}")
    print(f"\nClean shape:     {clean.shape}")
    print(f"Analytics shape: {analytics.shape}")
    print(analytics.head())
