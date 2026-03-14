"""Tests for src/watchlist_fetcher.py."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestWatchlistFetcher:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_db_path):
        """Set up WatchlistFetcher with mocked obb and temp DB."""
        with (
            patch("watchlist_fetcher.obb") as self.mock_obb,
            patch("database.DB_PATH", tmp_db_path),
            patch("config.DB_PATH", tmp_db_path),
            patch("config.DATA_DIR", tmp_db_path.parent),
            patch("config.CACHE_DIR", tmp_db_path.parent / "cache"),
        ):
            (tmp_db_path.parent / "cache").mkdir(exist_ok=True)
            from watchlist_fetcher import WatchlistFetcher

            self.fetcher = WatchlistFetcher()
            yield

    def _make_result(self, df):
        result = MagicMock()
        result.to_dataframe.return_value = df
        return result

    def test_flatten_watchlist(self):
        symbols = self.fetcher._flatten_watchlist()
        assert len(symbols) > 0
        assert symbols == sorted(set(symbols))

    def test_fetch_prices_success(self, sample_price_df):
        self.mock_obb.equity.price.historical.return_value = self._make_result(sample_price_df)
        result = self.fetcher.fetch_prices("AAPL", days=5)
        assert result is not None
        assert not result.empty
        assert "symbol" in result.columns
        assert result["symbol"].iloc[0] == "AAPL"

    def test_fetch_prices_api_error(self):
        self.mock_obb.equity.price.historical.side_effect = Exception("API error")
        result = self.fetcher.fetch_prices("AAPL")
        assert result is None

    def test_fetch_prices_none_response(self):
        self.mock_obb.equity.price.historical.return_value = None
        result = self.fetcher.fetch_prices("AAPL")
        assert result is None

    def test_fetch_fundamentals_success(self, sample_fundamentals_df):
        self.mock_obb.equity.fundamental.metrics.return_value = self._make_result(
            sample_fundamentals_df
        )
        result = self.fetcher.fetch_fundamentals("AAPL")
        assert result is not None
        assert "symbol" in result.columns

    def test_fetch_fundamentals_error(self):
        self.mock_obb.equity.fundamental.metrics.side_effect = Exception("Timeout")
        result = self.fetcher.fetch_fundamentals("AAPL")
        assert result is None

    def test_fetch_sec_filings_success(self, sample_filings_df):
        self.mock_obb.equity.fundamental.filings.return_value = self._make_result(sample_filings_df)
        result = self.fetcher.fetch_sec_filings("AAPL", limit=2)
        assert result is not None
        assert len(result) == 2

    def test_update_all_prices_calls_api(self):
        # Use unique dates per symbol to avoid UNIQUE constraint issues
        self.mock_obb.equity.price.historical.side_effect = Exception("API error")
        # Should not raise even with API failures
        self.fetcher.update_all_prices(days=5)
        assert self.mock_obb.equity.price.historical.call_count == len(self.fetcher.symbols)

    def test_update_all_fundamentals(self, sample_fundamentals_df):
        self.mock_obb.equity.fundamental.metrics.return_value = self._make_result(
            sample_fundamentals_df
        )
        self.fetcher.update_all_fundamentals()
        assert self.mock_obb.equity.fundamental.metrics.call_count == len(self.fetcher.symbols)

    def test_get_watchlist_summary_no_data(self):
        summary = self.fetcher.get_watchlist_summary()
        assert summary.empty

    def test_get_watchlist_summary_with_data(self):
        # Create price data with unique dates for this symbol
        dates = pd.date_range("2025-06-01", periods=5, freq="B")
        price_df = pd.DataFrame(
            {
                "date": dates,
                "open": [150.0, 151.0, 152.0, 153.0, 154.0],
                "high": [155.0, 156.0, 157.0, 158.0, 159.0],
                "low": [149.0, 150.0, 151.0, 152.0, 153.0],
                "close": [152.0, 153.0, 154.0, 155.0, 156.0],
                "volume": [1000000, 1100000, 1200000, 1300000, 1400000],
            }
        )
        self.fetcher.db.save_prices(price_df, "AAPL")
        summary = self.fetcher.get_watchlist_summary()
        assert isinstance(summary, pd.DataFrame)

    def test_get_watchlist_summary_calculates_change_pct(self):
        """Bug #11 regression: change_percent column didn't exist, always showed 0%.
        Verify that change_pct is correctly calculated from two price rows."""
        price_df = pd.DataFrame({
            "date": ["2025-06-01", "2025-06-02"],
            "open": [99.0, 104.0],
            "high": [101.0, 106.0],
            "low": [98.0, 103.0],
            "close": [100.0, 105.0],
            "volume": [1000000, 1100000],
        })
        self.fetcher.db.save_prices(price_df, "AAPL")
        summary = self.fetcher.get_watchlist_summary()
        assert not summary.empty
        aapl = summary[summary["symbol"] == "AAPL"]
        assert len(aapl) == 1
        assert aapl["change"].iloc[0] == pytest.approx(5.0)
        assert aapl["change_pct"].iloc[0] == pytest.approx(5.0)  # (105-100)/100 * 100

    def test_get_watchlist_summary_skips_single_row_symbol(self):
        """Bug #12 regression: single price row should not produce a summary entry
        (would have given misleading 0% change or crash without bounds check)."""
        price_df = pd.DataFrame({
            "date": ["2025-06-01"],
            "open": [99.0],
            "high": [101.0],
            "low": [98.0],
            "close": [100.0],
            "volume": [1000000],
        })
        self.fetcher.db.save_prices(price_df, "AAPL")
        summary = self.fetcher.get_watchlist_summary()
        # Symbol with only 1 row should be excluded (can't compute change)
        assert "AAPL" not in summary.get("symbol", pd.Series()).values
