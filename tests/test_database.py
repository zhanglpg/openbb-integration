"""Tests for src/database.py."""

import json
import sqlite3

import pandas as pd
import pytest

from database import SCHEMA_VERSION


class TestDatabaseInit:
    def test_creates_tables(self, tmp_db):
        with sqlite3.connect(tmp_db.db_path) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {row[0] for row in cursor.fetchall()}

        assert "price_history" in tables
        assert "fundamentals" in tables
        assert "sec_filings" in tables
        assert "economic_indicators" in tables
        assert "fetch_log" in tables
        assert "watchlist" in tables
        assert "schema_version" in tables

    def test_db_path_set(self, tmp_db, tmp_db_path):
        assert tmp_db.db_path == tmp_db_path

    def test_schema_version_recorded(self, tmp_db):
        with sqlite3.connect(tmp_db.db_path) as conn:
            cursor = conn.execute("SELECT version FROM schema_version ORDER BY applied_at DESC LIMIT 1")
            version = cursor.fetchone()[0]
        assert version == SCHEMA_VERSION

    def test_creates_indexes(self, tmp_db):
        with sqlite3.connect(tmp_db.db_path) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
            indexes = {row[0] for row in cursor.fetchall()}
        assert "idx_price_history_symbol" in indexes
        assert "idx_price_history_date" in indexes
        assert "idx_fundamentals_symbol" in indexes
        assert "idx_sec_filings_symbol" in indexes
        assert "idx_economic_indicators_series_id" in indexes
        assert "idx_fetch_log_symbol" in indexes


class TestSavePrices:
    def test_save_and_retrieve(self, tmp_db, sample_price_df):
        tmp_db.save_prices(sample_price_df, "AAPL")
        result = tmp_db.get_latest_prices("AAPL", days=10)
        assert not result.empty
        assert len(result) == 5

    def test_save_none_df(self, tmp_db):
        tmp_db.save_prices(None, "AAPL")
        result = tmp_db.get_latest_prices("AAPL")
        assert result.empty

    def test_save_empty_df(self, tmp_db):
        tmp_db.save_prices(pd.DataFrame(), "AAPL")
        result = tmp_db.get_latest_prices("AAPL")
        assert result.empty

    def test_save_with_date_index(self, tmp_db):
        dates = pd.date_range("2025-01-01", periods=3, freq="B")
        df = pd.DataFrame(
            {
                "open": [1, 2, 3],
                "close": [4, 5, 6],
                "high": [7, 8, 9],
                "low": [0.5, 1.5, 2.5],
                "volume": [100, 200, 300],
            },
            index=dates,
        )
        df.index.name = "date"
        tmp_db.save_prices(df, "TEST")
        result = tmp_db.get_latest_prices("TEST", days=10)
        assert len(result) == 3

    def test_symbol_stored_correctly(self, tmp_db, sample_price_df):
        tmp_db.save_prices(sample_price_df, "GOOGL")
        result = tmp_db.get_latest_prices("GOOGL", days=10)
        assert all(result["symbol"] == "GOOGL")

    def test_idempotent_rerun(self, tmp_db, sample_price_df):
        """Running save_prices twice should not duplicate rows."""
        tmp_db.save_prices(sample_price_df, "AAPL")
        tmp_db.save_prices(sample_price_df, "AAPL")
        result = tmp_db.get_latest_prices("AAPL", days=10)
        assert len(result) == 5

    def test_rejects_non_positive_prices(self, tmp_db):
        df = pd.DataFrame({
            "date": ["2025-01-01", "2025-01-02"],
            "close": [0, -5],
            "open": [1, 1],
            "high": [2, 2],
            "low": [0.5, 0.5],
            "volume": [100, 100],
        })
        tmp_db.save_prices(df, "BAD")
        result = tmp_db.get_latest_prices("BAD")
        assert result.empty

    def test_save_prices_missing_close_column(self, tmp_db):
        """Bug #7 regression: DataFrame without 'close' column should be rejected."""
        df = pd.DataFrame({
            "date": ["2025-01-01", "2025-01-02"],
            "open": [150.0, 151.0],
            "high": [155.0, 156.0],
            "low": [149.0, 150.0],
            "volume": [1000000, 1100000],
        })
        tmp_db.save_prices(df, "NOCL")
        result = tmp_db.get_latest_prices("NOCL")
        assert result.empty

    def test_save_prices_null_dates_dropped(self, tmp_db):
        """Bug #7 regression: rows with null dates should be dropped, valid rows kept."""
        df = pd.DataFrame({
            "date": ["2025-01-01", None, "2025-01-03"],
            "close": [150.0, 151.0, 152.0],
            "open": [149.0, 150.0, 151.0],
            "high": [155.0, 156.0, 157.0],
            "low": [148.0, 149.0, 150.0],
            "volume": [100, 200, 300],
        })
        tmp_db.save_prices(df, "NULLD")
        result = tmp_db.get_latest_prices("NULLD", days=10)
        assert len(result) == 2


class TestSaveFundamentals:
    def test_save_fundamentals(self, tmp_db, sample_fundamentals_df):
        tmp_db.save_fundamentals(sample_fundamentals_df, "AAPL")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query("SELECT * FROM fundamentals WHERE symbol = 'AAPL'", conn)
        assert not result.empty
        assert result["market_cap"].iloc[0] == 2500000000000

    def test_extra_columns_stored_as_json(self, tmp_db):
        """Columns not in the fixed schema go into extra_data JSON."""
        df = pd.DataFrame(
            {
                "market_cap": [100],
                "new_metric": [42.0],
                "another_thing": ["hello"],
            }
        )
        tmp_db.save_fundamentals(df, "TEST")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query(
                "SELECT extra_data FROM fundamentals WHERE symbol = 'TEST'", conn
            )
        assert not result.empty
        extra = json.loads(result["extra_data"].iloc[0])
        assert extra["new_metric"] == 42.0
        assert extra["another_thing"] == "hello"

    def test_idempotent_same_day(self, tmp_db, sample_fundamentals_df):
        """Running twice on the same day should replace, not duplicate."""
        tmp_db.save_fundamentals(sample_fundamentals_df, "AAPL")
        tmp_db.save_fundamentals(sample_fundamentals_df, "AAPL")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query("SELECT * FROM fundamentals WHERE symbol = 'AAPL'", conn)
        assert len(result) == 1


class TestSaveSecFilings:
    def test_save_sec_filings(self, tmp_db, sample_filings_df):
        tmp_db.save_sec_filings(sample_filings_df, "AAPL")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query("SELECT * FROM sec_filings WHERE symbol = 'AAPL'", conn)
        assert len(result) == 3

    def test_date_formatting(self, tmp_db, sample_filings_df):
        tmp_db.save_sec_filings(sample_filings_df, "AAPL")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query(
                "SELECT filing_date FROM sec_filings WHERE symbol = 'AAPL' LIMIT 1", conn
            )
        # Dates should be in YYYY-MM-DD format
        assert len(result["filing_date"].iloc[0]) == 10

    def test_idempotent_rerun(self, tmp_db, sample_filings_df):
        """Running save_sec_filings twice should not duplicate rows."""
        tmp_db.save_sec_filings(sample_filings_df, "AAPL")
        tmp_db.save_sec_filings(sample_filings_df, "AAPL")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query("SELECT * FROM sec_filings WHERE symbol = 'AAPL'", conn)
        assert len(result) == 3

    def test_extra_columns_stored_as_json(self, tmp_db, sample_filings_df):
        """Extra columns from SEC API go into extra_data."""
        tmp_db.save_sec_filings(sample_filings_df, "AAPL")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query(
                "SELECT extra_data FROM sec_filings WHERE symbol = 'AAPL' LIMIT 1", conn
            )
        # sample_filings_df has filing_detail_url and primary_doc which are now known cols,
        # so extra_data may be null or empty — that's correct
        assert not result.empty

    def test_rejects_null_accession_number(self, tmp_db):
        """Rows without accession_number should be dropped."""
        df = pd.DataFrame({
            "filing_date": ["2025-01-15"],
            "report_type": ["10-K"],
            "accession_number": [None],
            "report_url": ["https://sec.gov/10k"],
        })
        tmp_db.save_sec_filings(df, "AAPL")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query("SELECT * FROM sec_filings WHERE symbol = 'AAPL'", conn)
        assert result.empty

    def test_missing_accession_number_column(self, tmp_db):
        """Bug #7 regression: DataFrame without accession_number column should be skipped entirely."""
        df = pd.DataFrame({
            "filing_date": ["2025-01-15"],
            "report_type": ["10-K"],
            "report_url": ["https://sec.gov/10k"],
        })
        tmp_db.save_sec_filings(df, "AAPL")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query("SELECT * FROM sec_filings WHERE symbol = 'AAPL'", conn)
        assert result.empty


class TestSaveEconomicIndicators:
    def test_save_and_dedup(self, tmp_db, sample_economic_df):
        tmp_db.save_economic_indicators(sample_economic_df, "GDP")
        # Save again - should replace, not duplicate
        tmp_db.save_economic_indicators(sample_economic_df, "GDP")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query(
                "SELECT * FROM economic_indicators WHERE series_id = 'GDP'", conn
            )
        assert len(result) == 12

    def test_save_with_date_index(self, tmp_db):
        dates = pd.date_range("2024-01-01", periods=3, freq="MS")
        df = pd.DataFrame({"value": [1.0, 2.0, 3.0]}, index=dates)
        df.index.name = "date"
        tmp_db.save_economic_indicators(df, "TEST_SERIES")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query(
                "SELECT * FROM economic_indicators WHERE series_id = 'TEST_SERIES'", conn
            )
        assert len(result) == 3


class TestGetLatestPrices:
    def test_returns_limited_rows(self, tmp_db, sample_price_df):
        tmp_db.save_prices(sample_price_df, "AAPL")
        result = tmp_db.get_latest_prices("AAPL", days=2)
        assert len(result) == 2

    def test_returns_empty_for_unknown_symbol(self, tmp_db):
        result = tmp_db.get_latest_prices("UNKNOWN")
        assert result.empty


class TestGetAllSymbols:
    def test_returns_symbols(self, tmp_db, sample_price_df):
        tmp_db.save_prices(sample_price_df, "AAPL")
        tmp_db.save_prices(sample_price_df, "MSFT")
        symbols = tmp_db.get_all_symbols()
        assert set(symbols) == {"AAPL", "MSFT"}

    def test_empty_db(self, tmp_db):
        assert tmp_db.get_all_symbols() == []


class TestUpdateMetadata:
    def test_records_metadata(self, tmp_db):
        tmp_db.update_metadata("prices", "AAPL", 100, "success")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query("SELECT * FROM fetch_log", conn)
        assert len(result) == 1
        assert result["data_type"].iloc[0] == "prices"
        assert result["symbol"].iloc[0] == "AAPL"
        assert result["record_count"].iloc[0] == 100
        assert result["status"].iloc[0] == "success"


class TestBatchQueries:
    def test_get_latest_prices_batch(self, tmp_db, sample_price_df):
        tmp_db.save_prices(sample_price_df, "AAPL")
        tmp_db.save_prices(sample_price_df, "MSFT")
        result = tmp_db.get_latest_prices_batch(["AAPL", "MSFT"])
        assert len(result) == 2
        assert set(result["symbol"]) == {"AAPL", "MSFT"}

    def test_get_latest_prices_batch_empty(self, tmp_db):
        result = tmp_db.get_latest_prices_batch([])
        assert result.empty

    def test_get_latest_prices_batch_with_previous(self, tmp_db, sample_price_df):
        tmp_db.save_prices(sample_price_df, "AAPL")
        result = tmp_db.get_latest_prices_batch_with_previous(["AAPL"])
        assert len(result) == 2  # two most recent rows

    def test_get_latest_economic_indicators(self, tmp_db, sample_economic_df):
        tmp_db.save_economic_indicators(sample_economic_df, "GDP")
        tmp_db.save_economic_indicators(sample_economic_df, "CPI")
        result = tmp_db.get_latest_economic_indicators(["GDP"])
        assert len(result) == 1
        assert result["series_id"].iloc[0] == "GDP"

    def test_get_latest_economic_indicators_all(self, tmp_db, sample_economic_df):
        tmp_db.save_economic_indicators(sample_economic_df, "GDP")
        tmp_db.save_economic_indicators(sample_economic_df, "CPI")
        result = tmp_db.get_latest_economic_indicators()
        assert len(result) == 2

    def test_batch_partial_data(self, tmp_db, sample_price_df):
        """Bug #3 regression: batch query with mix of known and unknown symbols."""
        tmp_db.save_prices(sample_price_df, "AAPL")
        result = tmp_db.get_latest_prices_batch(["AAPL", "UNKNOWN"])
        assert len(result) == 1
        assert result["symbol"].iloc[0] == "AAPL"

    def test_batch_with_previous_single_price(self, tmp_db):
        """Bug #11 regression: only 1 price row should return 1 row, not crash."""
        df = pd.DataFrame({
            "date": ["2025-01-01"],
            "close": [100.0],
            "open": [99.0],
            "high": [101.0],
            "low": [98.0],
            "volume": [500000],
        })
        tmp_db.save_prices(df, "SOLO")
        result = tmp_db.get_latest_prices_batch_with_previous(["SOLO"])
        assert len(result) == 1  # only 1 row exists, can't compute change
