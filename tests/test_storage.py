"""Tests for src/storage.py."""

import sqlite3
from pathlib import Path

import pandas as pd
import pytest


class TestDataStorageInit:
    def test_creates_directories(self, tmp_storage):
        assert tmp_storage.prices_dir.is_dir()
        assert tmp_storage.fundamentals_dir.is_dir()
        assert tmp_storage.sec_dir.is_dir()

    def test_creates_db(self, tmp_storage):
        assert tmp_storage.db_path.exists()

    def test_creates_tables(self, tmp_storage):
        with sqlite3.connect(tmp_storage.db_path) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {row[0] for row in cursor.fetchall()}
        assert "watchlist" in tables
        assert "fetch_log" in tables
        assert "sec_filings" in tables


class TestSavePrices:
    def test_save_creates_parquet(self, tmp_storage, sample_price_df):
        path = tmp_storage.save_prices("AAPL", sample_price_df)
        assert path != ""
        assert Path(path).exists()
        assert path.endswith(".parquet")

    def test_save_empty_df(self, tmp_storage):
        result = tmp_storage.save_prices("AAPL", pd.DataFrame())
        assert result == ""

    def test_saved_data_readable(self, tmp_storage, sample_price_df):
        tmp_storage.save_prices("AAPL", sample_price_df)
        loaded = tmp_storage.load_prices("AAPL")
        assert not loaded.empty
        assert "_symbol" in loaded.columns
        assert loaded["_symbol"].iloc[0] == "AAPL"


class TestLoadPrices:
    def test_load_nonexistent(self, tmp_storage):
        result = tmp_storage.load_prices("NONEXISTENT")
        assert result.empty

    def test_load_with_limit(self, tmp_storage, sample_price_df):
        tmp_storage.save_prices("AAPL", sample_price_df)
        result = tmp_storage.load_prices("AAPL", limit=2)
        assert len(result) == 2


class TestSaveFundamentals:
    def test_save_creates_parquet(self, tmp_storage, sample_fundamentals_df):
        path = tmp_storage.save_fundamentals("AAPL", "metrics", sample_fundamentals_df)
        assert path != ""
        assert Path(path).exists()

    def test_save_empty_df(self, tmp_storage):
        result = tmp_storage.save_fundamentals("AAPL", "metrics", pd.DataFrame())
        assert result == ""


class TestLoadFundamentals:
    def test_load_saved_data(self, tmp_storage, sample_fundamentals_df):
        tmp_storage.save_fundamentals("AAPL", "metrics", sample_fundamentals_df)
        loaded = tmp_storage.load_fundamentals("AAPL", "metrics")
        assert not loaded.empty
        assert "_data_type" in loaded.columns

    def test_load_nonexistent(self, tmp_storage):
        result = tmp_storage.load_fundamentals("AAPL", "metrics")
        assert result.empty


class TestSaveSecFilings:
    def test_save_creates_parquet_and_db(self, tmp_storage, sample_filings_df):
        paths = tmp_storage.save_sec_filings("AAPL", sample_filings_df)
        assert len(paths) == 1
        assert Path(paths[0]).exists()

        # Verify DB records
        with sqlite3.connect(tmp_storage.db_path) as conn:
            result = pd.read_sql_query("SELECT * FROM sec_filings WHERE symbol = 'AAPL'", conn)
        assert len(result) == 3

    def test_save_empty_df(self, tmp_storage):
        result = tmp_storage.save_sec_filings("AAPL", pd.DataFrame())
        assert result == []


class TestLogFetch:
    def test_log_creates_record(self, tmp_storage):
        tmp_storage.log_fetch("AAPL", "prices", "yfinance", "success", record_count=100)
        history = tmp_storage.get_fetch_history(symbol="AAPL")
        assert len(history) == 1
        assert history["status"].iloc[0] == "success"

    def test_log_with_error(self, tmp_storage):
        tmp_storage.log_fetch("AAPL", "prices", "yfinance", "error", "API timeout", 0)
        history = tmp_storage.get_fetch_history(symbol="AAPL")
        assert history["error_message"].iloc[0] == "API timeout"


class TestGetFetchHistory:
    def test_filter_by_symbol(self, tmp_storage):
        tmp_storage.log_fetch("AAPL", "prices", "yfinance", "success", record_count=10)
        tmp_storage.log_fetch("MSFT", "prices", "yfinance", "success", record_count=20)
        result = tmp_storage.get_fetch_history(symbol="AAPL")
        assert len(result) == 1
        assert result["symbol"].iloc[0] == "AAPL"

    def test_no_filter_returns_all(self, tmp_storage):
        tmp_storage.log_fetch("AAPL", "prices", "yfinance", "success", record_count=10)
        tmp_storage.log_fetch("MSFT", "prices", "yfinance", "success", record_count=20)
        result = tmp_storage.get_fetch_history()
        assert len(result) == 2

    def test_limit(self, tmp_storage):
        for i in range(5):
            tmp_storage.log_fetch(f"SYM{i}", "prices", "yfinance", "success", record_count=i)
        result = tmp_storage.get_fetch_history(limit=3)
        assert len(result) == 3


class TestWatchlist:
    def test_add_and_get(self, tmp_storage):
        tmp_storage.update_watchlist("AAPL", name="Apple Inc", sector="Tech")
        result = tmp_storage.get_watchlist()
        assert len(result) == 1
        assert result["symbol"].iloc[0] == "AAPL"

    def test_update_existing(self, tmp_storage):
        tmp_storage.update_watchlist("AAPL", name="Apple")
        tmp_storage.update_watchlist("AAPL", name="Apple Inc", sector="Technology")
        result = tmp_storage.get_watchlist()
        assert len(result) == 1
        assert result["name"].iloc[0] == "Apple Inc"


# ===================================================================
# Error paths and edge cases
# ===================================================================


class TestLoadPricesErrorPaths:
    def test_load_corrupted_parquet(self, tmp_storage):
        """Corrupted parquet file raises an error (not silent)."""
        # Write garbage to where a parquet file would be
        corrupt_file = tmp_storage.prices_dir / "AAPL_prices_20250101.parquet"
        corrupt_file.write_bytes(b"this is not a parquet file")
        with pytest.raises(Exception):
            tmp_storage.load_prices("AAPL")

    def test_load_fundamentals_corrupted(self, tmp_storage):
        """Corrupted fundamentals parquet raises an error."""
        corrupt_file = tmp_storage.fundamentals_dir / "AAPL_metrics_20250101.parquet"
        corrupt_file.write_bytes(b"corrupt data")
        with pytest.raises(Exception):
            tmp_storage.load_fundamentals("AAPL", "metrics")


class TestSaveEdgeCases:
    def test_save_prices_creates_metadata_columns(self, tmp_storage, sample_price_df):
        """Saved data includes _symbol and _fetched_at metadata."""
        tmp_storage.save_prices("AAPL", sample_price_df)
        loaded = tmp_storage.load_prices("AAPL")
        assert "_symbol" in loaded.columns
        assert "_fetched_at" in loaded.columns

    def test_save_sec_filings_empty_df(self, tmp_storage):
        """Empty SEC filings DF returns empty list, no crash."""
        result = tmp_storage.save_sec_filings("AAPL", pd.DataFrame())
        assert result == []

    def test_save_fundamentals_none_values(self, tmp_storage):
        """DataFrame with None values saves successfully."""
        df = pd.DataFrame({"metric": [None, "test"], "value": [1.0, None]})
        path = tmp_storage.save_fundamentals("AAPL", "metrics", df)
        assert path != ""
        loaded = tmp_storage.load_fundamentals("AAPL", "metrics")
        assert not loaded.empty


class TestFetchHistoryErrorPaths:
    def test_log_failed_fetch_and_retrieve(self, tmp_storage):
        """Error fetches are logged and retrievable."""
        tmp_storage.log_fetch("AAPL", "prices", "yfinance", "error", "Connection timeout", 0)
        tmp_storage.log_fetch("AAPL", "prices", "yfinance", "success", record_count=100)
        history = tmp_storage.get_fetch_history(symbol="AAPL")
        assert len(history) == 2
        statuses = list(history["status"])
        assert "error" in statuses
        assert "success" in statuses

    def test_fetch_history_multiple_types(self, tmp_storage):
        """Fetch history includes all data types."""
        tmp_storage.log_fetch("AAPL", "prices", "yfinance", "success", record_count=100)
        tmp_storage.log_fetch("AAPL", "fundamentals", "yfinance", "success", record_count=5)
        history = tmp_storage.get_fetch_history(symbol="AAPL")
        assert len(history) == 2
        types = set(history["data_type"])
        assert types == {"prices", "fundamentals"}
