"""Tests for src/storage.py."""

import sqlite3
from pathlib import Path

import pandas as pd


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
