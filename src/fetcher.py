"""
Data Fetcher - Fetch financial data from OpenBB
"""

import logging
from typing import Optional

import pandas as pd
from openbb import obb

from retry import retry_fetch

logger = logging.getLogger(__name__)


class DataFetcher:
    """Fetch financial data using OpenBB SDK"""

    def __init__(self, output_type: str = "dataframe"):
        obb.user.preferences.output_type = output_type

    def fetch_historical_prices(
        self,
        symbol: str,
        provider: str = "yfinance",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch historical price data."""
        try:
            def _call():
                return obb.equity.price.historical(
                    symbol, provider=provider, start_date=start_date, end_date=end_date
                )

            result = retry_fetch(_call, description=f"prices for {symbol}")
            if hasattr(result, "to_dataframe"):
                return result.to_dataframe()
            return result
        except Exception as e:
            logger.error("Error fetching prices for %s: %s", symbol, e)
            return pd.DataFrame()

    def fetch_income_statement(self, symbol: str, provider: str = "yfinance") -> pd.DataFrame:
        """Fetch income statement."""
        try:
            def _call():
                return obb.equity.fundamental.income(symbol, provider=provider)

            result = retry_fetch(_call, description=f"income for {symbol}")
            if hasattr(result, "to_dataframe"):
                return result.to_dataframe()
            return result
        except Exception as e:
            logger.error("Error fetching income for %s: %s", symbol, e)
            return pd.DataFrame()

    def fetch_balance_sheet(self, symbol: str, provider: str = "yfinance") -> pd.DataFrame:
        """Fetch balance sheet."""
        try:
            def _call():
                return obb.equity.fundamental.balance(symbol, provider=provider)

            result = retry_fetch(_call, description=f"balance for {symbol}")
            if hasattr(result, "to_dataframe"):
                return result.to_dataframe()
            return result
        except Exception as e:
            logger.error("Error fetching balance for %s: %s", symbol, e)
            return pd.DataFrame()

    def fetch_cash_flow(self, symbol: str, provider: str = "yfinance") -> pd.DataFrame:
        """Fetch cash flow statement."""
        try:
            def _call():
                return obb.equity.fundamental.cash(symbol, provider=provider)

            result = retry_fetch(_call, description=f"cash flow for {symbol}")
            if hasattr(result, "to_dataframe"):
                return result.to_dataframe()
            return result
        except Exception as e:
            logger.error("Error fetching cash flow for %s: %s", symbol, e)
            return pd.DataFrame()

    def fetch_metrics(self, symbol: str, provider: str = "yfinance") -> pd.DataFrame:
        """Fetch key financial metrics."""
        try:
            def _call():
                return obb.equity.fundamental.metrics(symbol, provider=provider)

            result = retry_fetch(_call, description=f"metrics for {symbol}")
            if hasattr(result, "to_dataframe"):
                return result.to_dataframe()
            return result
        except Exception as e:
            logger.error("Error fetching metrics for %s: %s", symbol, e)
            return pd.DataFrame()

    def fetch_sec_filings(
        self, symbol: str, provider: str = "sec", limit: int = 100
    ) -> pd.DataFrame:
        """Fetch SEC filings."""
        try:
            def _call():
                return obb.equity.fundamental.filings(symbol, provider=provider)

            result = retry_fetch(_call, description=f"SEC filings for {symbol}")
            if hasattr(result, "to_dataframe"):
                df = result.to_dataframe()
                return df.head(limit) if limit else df
            return result
        except Exception as e:
            logger.error("Error fetching SEC filings for %s: %s", symbol, e)
            return pd.DataFrame()

    def fetch_profile(self, symbol: str, provider: str = "yfinance") -> dict:
        """Fetch company profile."""
        try:
            def _call():
                return obb.equity.profile(symbol, provider=provider)

            result = retry_fetch(_call, description=f"profile for {symbol}")
            if hasattr(result, "results") and result.results:
                return result.results[0] if isinstance(result.results, list) else result.results
            return {}
        except Exception as e:
            logger.error("Error fetching profile for %s: %s", symbol, e)
            return {}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    fetcher = DataFetcher()

    logger.info("=== Testing Historical Prices ===")
    df = fetcher.fetch_historical_prices("AAPL")
    logger.info("Shape: %s", df.shape)
    logger.info("Columns: %s", list(df.columns))
    print(df.tail(3))

    logger.info("=== Testing Metrics ===")
    metrics = fetcher.fetch_metrics("AAPL")
    logger.info("Shape: %s", metrics.shape)
    print(metrics.to_string())
