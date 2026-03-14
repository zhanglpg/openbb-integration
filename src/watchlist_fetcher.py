"""Watchlist data fetcher - fetches prices and fundamentals for tracked symbols."""

import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
from openbb import obb

from config import PIPELINE_DEFAULTS, WATCHLIST
from database import Database
from retry import retry_fetch

logger = logging.getLogger(__name__)


class WatchlistFetcher:
    """Fetches and stores watchlist data from OpenBB."""

    def __init__(self):
        self.db = Database()
        self.symbols = self._flatten_watchlist()
        obb.user.preferences.output_type = "dataframe"

    def _flatten_watchlist(self) -> List[str]:
        """Flatten watchlist categories into unique symbols."""
        symbols = set()
        for category, syms in WATCHLIST.items():
            symbols.update(syms)
        return sorted(list(symbols))

    def fetch_prices(self, symbol: str, days: int = None) -> Optional[pd.DataFrame]:
        """Fetch historical prices for a symbol."""
        if days is None:
            days = PIPELINE_DEFAULTS["price_lookback_days"]
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            def _call():
                return obb.equity.price.historical(
                    symbol=symbol,
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                )

            data = retry_fetch(_call, description=f"prices for {symbol}")

            if data is not None:
                df = data.to_dataframe() if hasattr(data, "to_dataframe") else data
                df["symbol"] = symbol
                df["fetched_at"] = datetime.now().isoformat()
                return df
        except Exception as e:
            logger.error("Error fetching prices for %s: %s", symbol, e)
        return None

    def fetch_fundamentals(self, symbol: str) -> Optional[pd.DataFrame]:
        """Fetch fundamental metrics for a symbol."""
        try:
            def _call():
                return obb.equity.fundamental.metrics(symbol=symbol)

            data = retry_fetch(_call, description=f"fundamentals for {symbol}")

            if data is not None:
                df = data.to_dataframe() if hasattr(data, "to_dataframe") else data
                df["symbol"] = symbol
                df["fetched_at"] = datetime.now().isoformat()
                return df
        except Exception as e:
            logger.error("Error fetching fundamentals for %s: %s", symbol, e)
        return None

    def fetch_sec_filings(self, symbol: str, limit: int = None) -> Optional[pd.DataFrame]:
        """Fetch SEC filings for a symbol."""
        if limit is None:
            limit = PIPELINE_DEFAULTS["sec_filing_limit"]
        try:
            def _call():
                return obb.equity.fundamental.filings(symbol=symbol, provider="sec")

            data = retry_fetch(_call, description=f"SEC filings for {symbol}")

            if data is not None:
                df = data.to_dataframe() if hasattr(data, "to_dataframe") else data
                df = df.head(limit)
                df["symbol"] = symbol
                df["fetched_at"] = datetime.now().isoformat()
                return df
        except Exception as e:
            logger.error("Error fetching SEC filings for %s: %s", symbol, e)
        return None

    def update_all_prices(self, days: int = None):
        """Update prices for all watchlist symbols."""
        if days is None:
            days = PIPELINE_DEFAULTS["price_lookback_days"]
        delay = PIPELINE_DEFAULTS["api_call_delay"]

        logger.info("Updating prices for %d symbols...", len(self.symbols))

        for i, symbol in enumerate(self.symbols):
            logger.info("  Fetching %s...", symbol)
            df = self.fetch_prices(symbol, days)

            if df is not None and not df.empty:
                self.db.save_prices(df, symbol)
                self.db.update_metadata("prices", symbol, len(df), "success")
                logger.info("  %s: %d rows saved", symbol, len(df))
            else:
                self.db.update_metadata("prices", symbol, 0, "failed")
                logger.warning("  %s: no data returned", symbol)

            # Rate-limit between API calls (skip after last symbol)
            if delay and i < len(self.symbols) - 1:
                time.sleep(delay)

    def update_all_fundamentals(self):
        """Update fundamentals for all watchlist symbols."""
        delay = PIPELINE_DEFAULTS["api_call_delay"]

        logger.info("Updating fundamentals for %d symbols...", len(self.symbols))

        for i, symbol in enumerate(self.symbols):
            logger.info("  Fetching %s...", symbol)
            df = self.fetch_fundamentals(symbol)

            if df is not None and not df.empty:
                self.db.save_fundamentals(df, symbol)
                self.db.update_metadata("fundamentals", symbol, len(df), "success")
                logger.info("  %s: saved", symbol)
            else:
                self.db.update_metadata("fundamentals", symbol, 0, "failed")
                logger.warning("  %s: no data returned", symbol)

            if delay and i < len(self.symbols) - 1:
                time.sleep(delay)

    def update_all_sec_filings(self, limit: int = None):
        """Update SEC filings for all watchlist symbols."""
        if limit is None:
            limit = PIPELINE_DEFAULTS["sec_filing_limit"]
        delay = PIPELINE_DEFAULTS["api_call_delay"]

        logger.info("Updating SEC filings for %d symbols...", len(self.symbols))

        for i, symbol in enumerate(self.symbols):
            logger.info("  Fetching %s...", symbol)
            df = self.fetch_sec_filings(symbol, limit)

            if df is not None and not df.empty:
                self.db.save_sec_filings(df, symbol)
                self.db.update_metadata("sec_filings", symbol, len(df), "success")
                logger.info("  %s: %d rows saved", symbol, len(df))
            else:
                self.db.update_metadata("sec_filings", symbol, 0, "failed")
                logger.warning("  %s: no data returned", symbol)

            if delay and i < len(self.symbols) - 1:
                time.sleep(delay)

    def get_watchlist_summary(self) -> pd.DataFrame:
        """Get summary of all watchlist symbols with latest prices (batch query)."""
        df = self.db.get_latest_prices_batch_with_previous(self.symbols)
        if df.empty:
            return pd.DataFrame()

        summaries = []
        for symbol in self.symbols:
            symbol_df = df[df["symbol"] == symbol]
            if len(symbol_df) >= 2:
                latest = symbol_df.iloc[0]
                previous = symbol_df.iloc[1]
                change = latest["close"] - previous["close"]
                change_pct = (change / previous["close"]) * 100

                summaries.append(
                    {
                        "symbol": symbol,
                        "latest_close": latest["close"],
                        "previous_close": previous["close"],
                        "change": change,
                        "change_pct": change_pct,
                        "volume": latest["volume"],
                        "date": latest["date"],
                    }
                )

        return pd.DataFrame(summaries)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    fetcher = WatchlistFetcher()

    logger.info("=" * 60)
    logger.info("OpenBB Watchlist Fetcher")
    logger.info("=" * 60)
    logger.info("Symbols to track: %s", ", ".join(fetcher.symbols))

    # Update all data
    fetcher.update_all_prices()
    fetcher.update_all_fundamentals()
    fetcher.update_all_sec_filings()

    # Show summary
    logger.info("=" * 60)
    logger.info("Watchlist Summary")
    logger.info("=" * 60)
    summary = fetcher.get_watchlist_summary()
    print(summary.to_string(index=False))
