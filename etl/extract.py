import requests
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "demo")
BASE_URL = "https://www.alphavantage.co/query"
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]


def extract_stock_data(ticker: str, days_back: int = 30) -> pd.DataFrame:
    logger.info(f"Extracting data for ticker: {ticker}")
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "outputsize": "compact",
        "apikey": API_KEY
    }
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "Error Message" in data:
            logger.error(f"API error for {ticker}: {data['Error Message']}")
            return pd.DataFrame()

        if "Note" in data:
            logger.warning(f"API rate limit hit for {ticker}")
            return pd.DataFrame()

        time_series = data.get("Time Series (Daily)", {})
        if not time_series:
            logger.warning(f"No data returned for {ticker}")
            return pd.DataFrame()

        records = []
        cutoff_date = datetime.now() - timedelta(days=days_back)

        for date_str, prices in time_series.items():
            price_date = datetime.strptime(date_str, "%Y-%m-%d")
            if price_date < cutoff_date:
                continue
            records.append({
                "ticker":       ticker,
                "price_date":   date_str,
                "open_price":   float(prices["1. open"]),
                "high_price":   float(prices["2. high"]),
                "low_price":    float(prices["3. low"]),
                "close_price":  float(prices["4. close"]),
                "volume":       int(prices["5. volume"]),
                "source":       "alpha_vantage"
            })

        df = pd.DataFrame(records)
        logger.info(f"Extracted {len(df)} rows for {ticker}")
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {ticker}: {e}")
        return pd.DataFrame()


def extract_all_tickers(tickers: list = TICKERS) -> pd.DataFrame:
    logger.info(f"Starting extraction for {len(tickers)} tickers")
    all_data = []
    for ticker in tickers:
        df = extract_stock_data(ticker)
        if not df.empty:
            all_data.append(df)

    if not all_data:
        logger.error("No data extracted for any ticker")
        return pd.DataFrame()

    combined = pd.concat(all_data, ignore_index=True)
    logger.info(f"Total rows extracted: {len(combined)}")
    return combined


if __name__ == "__main__":
    df = extract_all_tickers()
    print(df.head(10))
    print(f"Shape: {df.shape}")
