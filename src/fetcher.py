"""
Data Fetcher - Fetch financial data from OpenBB
"""

from typing import Optional

import pandas as pd
from openbb import obb


class DataFetcher:
    """Fetch financial data using OpenBB SDK"""

    def __init__(self, output_type: str = "dataframe"):
        """
        Initialize data fetcher

        Args:
            output_type: Default output type ('dataframe' or 'obbject')
        """
        obb.user.preferences.output_type = output_type

    def fetch_historical_prices(
        self,
        symbol: str,
        provider: str = "yfinance",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Fetch historical price data

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            provider: Data provider ('yfinance', 'polygon', 'fmp')
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            DataFrame with OHLCV data
        """
        try:
            result = obb.equity.price.historical(
                symbol, provider=provider, start_date=start_date, end_date=end_date
            )
            if hasattr(result, "to_dataframe"):
                return result.to_dataframe()
            return result
        except Exception as e:
            print(f"Error fetching prices for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_income_statement(self, symbol: str, provider: str = "yfinance") -> pd.DataFrame:
        """
        Fetch income statement

        Args:
            symbol: Stock symbol
            provider: Data provider ('yfinance', 'fmp', 'intrinio')

        Returns:
            DataFrame with income statement data
        """
        try:
            result = obb.equity.fundamental.income(symbol, provider=provider)
            if hasattr(result, "to_dataframe"):
                return result.to_dataframe()
            return result
        except Exception as e:
            print(f"Error fetching income for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_balance_sheet(self, symbol: str, provider: str = "yfinance") -> pd.DataFrame:
        """
        Fetch balance sheet

        Args:
            symbol: Stock symbol
            provider: Data provider

        Returns:
            DataFrame with balance sheet data
        """
        try:
            result = obb.equity.fundamental.balance(symbol, provider=provider)
            if hasattr(result, "to_dataframe"):
                return result.to_dataframe()
            return result
        except Exception as e:
            print(f"Error fetching balance for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_cash_flow(self, symbol: str, provider: str = "yfinance") -> pd.DataFrame:
        """
        Fetch cash flow statement

        Args:
            symbol: Stock symbol
            provider: Data provider

        Returns:
            DataFrame with cash flow data
        """
        try:
            result = obb.equity.fundamental.cash(symbol, provider=provider)
            if hasattr(result, "to_dataframe"):
                return result.to_dataframe()
            return result
        except Exception as e:
            print(f"Error fetching cash flow for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_metrics(self, symbol: str, provider: str = "yfinance") -> pd.DataFrame:
        """
        Fetch key financial metrics

        Args:
            symbol: Stock symbol
            provider: Data provider

        Returns:
            DataFrame with metrics (P/E, market cap, ratios, etc.)
        """
        try:
            result = obb.equity.fundamental.metrics(symbol, provider=provider)
            if hasattr(result, "to_dataframe"):
                return result.to_dataframe()
            return result
        except Exception as e:
            print(f"Error fetching metrics for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_sec_filings(
        self, symbol: str, provider: str = "sec", limit: int = 100
    ) -> pd.DataFrame:
        """
        Fetch SEC filings

        Args:
            symbol: Stock symbol
            provider: Data provider ('sec', 'fmp', 'intrinio')
            limit: Maximum number of filings to return

        Returns:
            DataFrame with filing metadata
        """
        try:
            result = obb.equity.fundamental.filings(symbol, provider=provider)
            if hasattr(result, "to_dataframe"):
                df = result.to_dataframe()
                return df.head(limit) if limit else df
            return result
        except Exception as e:
            print(f"Error fetching SEC filings for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_profile(self, symbol: str, provider: str = "yfinance") -> dict:
        """
        Fetch company profile

        Args:
            symbol: Stock symbol
            provider: Data provider

        Returns:
            Dictionary with company information
        """
        try:
            result = obb.equity.profile(symbol, provider=provider)
            if hasattr(result, "results") and result.results:
                return result.results[0] if isinstance(result.results, list) else result.results
            return {}
        except Exception as e:
            print(f"Error fetching profile for {symbol}: {e}")
            return {}


if __name__ == "__main__":
    # Test
    fetcher = DataFetcher()

    print("=== Testing Historical Prices ===")
    df = fetcher.fetch_historical_prices("AAPL")
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(df.tail(3))

    print("\n=== Testing Metrics ===")
    metrics = fetcher.fetch_metrics("AAPL")
    print(f"Shape: {metrics.shape}")
    print(metrics.to_string())
