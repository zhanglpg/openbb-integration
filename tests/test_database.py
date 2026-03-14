"""Tests for src/database.py."""

import sqlite3

import pandas as pd
import pytest

from database import _sanitize_column_name


class TestSanitizeColumnName:
    def test_valid_simple_name(self):
        assert _sanitize_column_name("close") == '"close"'

    def test_valid_underscore_name(self):
        assert _sanitize_column_name("adj_close") == '"adj_close"'

    def test_valid_leading_underscore(self):
        assert _sanitize_column_name("_private") == '"_private"'

    def test_invalid_name_with_space(self):
        with pytest.raises(ValueError, match="Invalid column name"):
            _sanitize_column_name("bad name")

    def test_invalid_name_with_semicolon(self):
        with pytest.raises(ValueError, match="Invalid column name"):
            _sanitize_column_name("x; DROP TABLE")

    def test_invalid_name_starting_with_number(self):
        with pytest.raises(ValueError, match="Invalid column name"):
            _sanitize_column_name("1col")


class TestDatabaseInit:
    def test_creates_tables(self, tmp_db):
        with sqlite3.connect(tmp_db.db_path) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {row[0] for row in cursor.fetchall()}

        assert "price_history" in tables
        assert "fundamentals" in tables
        assert "sec_filings" in tables
        assert "economic_indicators" in tables
        assert "fetch_metadata" in tables

    def test_db_path_set(self, tmp_db, tmp_db_path):
        assert tmp_db.db_path == tmp_db_path


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


class TestSaveFundamentals:
    def test_save_fundamentals(self, tmp_db, sample_fundamentals_df):
        tmp_db.save_fundamentals(sample_fundamentals_df, "AAPL")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query("SELECT * FROM fundamentals WHERE symbol = 'AAPL'", conn)
        assert not result.empty
        assert result["market_cap"].iloc[0] == 2500000000000

    def test_dynamic_column_addition(self, tmp_db):
        df = pd.DataFrame(
            {
                "market_cap": [100],
                "new_metric": [42.0],
            }
        )
        tmp_db.save_fundamentals(df, "TEST")
        with sqlite3.connect(tmp_db.db_path) as conn:
            cursor = conn.execute("PRAGMA table_info(fundamentals)")
            columns = {row[1] for row in cursor.fetchall()}
        assert "new_metric" in columns

    def test_rejects_invalid_column_name(self, tmp_db):
        df = pd.DataFrame({"market_cap": [100], "bad col": [42.0]})
        with pytest.raises(ValueError, match="Invalid column name"):
            tmp_db.save_fundamentals(df, "TEST")


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
            result = pd.read_sql_query("SELECT * FROM fetch_metadata", conn)
        assert len(result) == 1
        assert result["data_type"].iloc[0] == "prices"
        assert result["symbol"].iloc[0] == "AAPL"
        assert result["rows_fetched"].iloc[0] == 100
        assert result["status"].iloc[0] == "success"
