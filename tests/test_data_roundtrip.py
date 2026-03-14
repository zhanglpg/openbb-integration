"""Integration tests for data round-trip: fetch -> store -> retrieve."""

import sqlite3

import pandas as pd
import pytest


@pytest.mark.integration
class TestDatabaseRoundTrip:
    def test_price_roundtrip(self, tmp_db, sample_price_df):
        """Save prices and retrieve them correctly."""
        tmp_db.save_prices(sample_price_df, "AAPL")
        result = tmp_db.get_latest_prices("AAPL", days=10)

        assert len(result) == 5
        assert all(result["symbol"] == "AAPL")
        # Verify data integrity
        assert result["close"].max() == 156.0
        assert result["close"].min() == 152.0

    def test_multi_symbol_roundtrip(self, tmp_db, sample_price_df):
        """Save prices for multiple symbols and retrieve independently."""
        tmp_db.save_prices(sample_price_df, "AAPL")
        tmp_db.save_prices(sample_price_df, "GOOGL")

        aapl = tmp_db.get_latest_prices("AAPL", days=10)
        googl = tmp_db.get_latest_prices("GOOGL", days=10)

        assert len(aapl) == 5
        assert len(googl) == 5
        assert all(aapl["symbol"] == "AAPL")
        assert all(googl["symbol"] == "GOOGL")

    def test_fundamentals_roundtrip(self, tmp_db, sample_fundamentals_df):
        """Save and retrieve fundamentals."""
        tmp_db.save_fundamentals(sample_fundamentals_df, "AAPL")

        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query("SELECT * FROM fundamentals WHERE symbol = 'AAPL'", conn)

        assert not result.empty
        assert result["pe_ratio"].iloc[0] == pytest.approx(28.5)
        assert result["market_cap"].iloc[0] == pytest.approx(2500000000000)

    def test_sec_filings_roundtrip(self, tmp_db, sample_filings_df):
        """Save and retrieve SEC filings."""
        tmp_db.save_sec_filings(sample_filings_df, "AAPL")

        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query("SELECT * FROM sec_filings WHERE symbol = 'AAPL'", conn)

        assert len(result) == 3
        report_types = set(result["report_type"])
        assert "10-K" in report_types
        assert "10-Q" in report_types

    def test_economic_indicators_roundtrip(self, tmp_db, sample_economic_df):
        """Save and retrieve economic indicators."""
        tmp_db.save_economic_indicators(sample_economic_df, "GDP")

        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query(
                "SELECT * FROM economic_indicators WHERE series_id = 'GDP'", conn
            )

        assert len(result) == 12
        assert all(result["series_id"] == "GDP")

    def test_metadata_tracking(self, tmp_db, sample_price_df):
        """Verify metadata is tracked correctly across operations."""
        tmp_db.save_prices(sample_price_df, "AAPL")
        tmp_db.update_metadata("prices", "AAPL", 5, "success")

        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query("SELECT * FROM fetch_metadata", conn)

        assert len(result) == 1
        assert result["data_type"].iloc[0] == "prices"
        assert result["rows_fetched"].iloc[0] == 5


@pytest.mark.integration
class TestStorageRoundTrip:
    def test_price_parquet_roundtrip(self, tmp_storage, sample_price_df):
        """Save prices to parquet and load them back."""
        tmp_storage.save_prices("AAPL", sample_price_df)
        loaded = tmp_storage.load_prices("AAPL")

        assert not loaded.empty
        assert len(loaded) == 5
        assert loaded["_symbol"].iloc[0] == "AAPL"

    def test_fundamentals_parquet_roundtrip(self, tmp_storage, sample_fundamentals_df):
        """Save fundamentals to parquet and load them back."""
        tmp_storage.save_fundamentals("AAPL", "metrics", sample_fundamentals_df)
        loaded = tmp_storage.load_fundamentals("AAPL", "metrics")

        assert not loaded.empty
        assert loaded["_data_type"].iloc[0] == "metrics"

    def test_sec_filings_dual_storage(self, tmp_storage, sample_filings_df):
        """Verify SEC filings are saved to both parquet and SQLite."""
        paths = tmp_storage.save_sec_filings("AAPL", sample_filings_df)

        # Check parquet
        assert len(paths) == 1
        loaded = pd.read_parquet(paths[0])
        assert len(loaded) == 3

        # Check SQLite
        with sqlite3.connect(tmp_storage.db_path) as conn:
            db_result = pd.read_sql_query("SELECT * FROM sec_filings WHERE symbol = 'AAPL'", conn)
        assert len(db_result) == 3

    def test_fetch_log_roundtrip(self, tmp_storage):
        """Log fetch operations and retrieve history."""
        tmp_storage.log_fetch("AAPL", "prices", "yfinance", "success", record_count=100)
        tmp_storage.log_fetch("MSFT", "prices", "yfinance", "error", "Timeout", 0)

        history = tmp_storage.get_fetch_history()
        assert len(history) == 2

        aapl_history = tmp_storage.get_fetch_history(symbol="AAPL")
        assert len(aapl_history) == 1
        assert aapl_history["status"].iloc[0] == "success"

    def test_watchlist_crud(self, tmp_storage):
        """Test watchlist create, read, update operations."""
        # Create
        tmp_storage.update_watchlist("AAPL", name="Apple Inc", sector="Technology")
        tmp_storage.update_watchlist("MSFT", name="Microsoft", sector="Technology")

        # Read
        watchlist = tmp_storage.get_watchlist()
        assert len(watchlist) == 2

        # Update
        tmp_storage.update_watchlist(
            "AAPL", name="Apple Inc.", sector="Technology", industry="Consumer Electronics"
        )
        watchlist = tmp_storage.get_watchlist()
        assert len(watchlist) == 2
        aapl_row = watchlist[watchlist["symbol"] == "AAPL"].iloc[0]
        assert aapl_row["name"] == "Apple Inc."
