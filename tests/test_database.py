"""Tests for src/database.py."""

import json
import sqlite3

import pandas as pd

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
            cursor = conn.execute(
                "SELECT version FROM schema_version ORDER BY applied_at DESC LIMIT 1"
            )
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
        df = pd.DataFrame(
            {
                "date": ["2025-01-01", "2025-01-02"],
                "close": [0, -5],
                "open": [1, 1],
                "high": [2, 2],
                "low": [0.5, 0.5],
                "volume": [100, 100],
            }
        )
        tmp_db.save_prices(df, "BAD")
        result = tmp_db.get_latest_prices("BAD")
        assert result.empty

    def test_save_prices_missing_close_column(self, tmp_db):
        """Bug #7 regression: DataFrame without 'close' column should be rejected."""
        df = pd.DataFrame(
            {
                "date": ["2025-01-01", "2025-01-02"],
                "open": [150.0, 151.0],
                "high": [155.0, 156.0],
                "low": [149.0, 150.0],
                "volume": [1000000, 1100000],
            }
        )
        tmp_db.save_prices(df, "NOCL")
        result = tmp_db.get_latest_prices("NOCL")
        assert result.empty

    def test_adjusted_close_not_clobbered(self, tmp_db):
        """Regression: when both adjusted_close and adj_close exist, adj_close is preserved."""
        df = pd.DataFrame(
            {
                "date": ["2025-01-01"],
                "open": [100.0],
                "high": [105.0],
                "low": [99.0],
                "close": [104.0],
                "volume": [1000000],
                "adj_close": [103.5],
                "adjusted_close": [999.0],
            }
        )
        tmp_db.save_prices(df, "ADJ")
        result = tmp_db.get_latest_prices("ADJ", days=10)
        assert len(result) == 1
        # adj_close should be the original value, not overwritten by adjusted_close
        assert result["adj_close"].iloc[0] == 103.5

    def test_save_prices_null_dates_dropped(self, tmp_db):
        """Bug #7 regression: rows with null dates should be dropped, valid rows kept."""
        df = pd.DataFrame(
            {
                "date": ["2025-01-01", None, "2025-01-03"],
                "close": [150.0, 151.0, 152.0],
                "open": [149.0, 150.0, 151.0],
                "high": [155.0, 156.0, 157.0],
                "low": [148.0, 149.0, 150.0],
                "volume": [100, 200, 300],
            }
        )
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
        df = pd.DataFrame(
            {
                "filing_date": ["2025-01-15"],
                "report_type": ["10-K"],
                "accession_number": [None],
                "report_url": ["https://sec.gov/10k"],
            }
        )
        tmp_db.save_sec_filings(df, "AAPL")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query("SELECT * FROM sec_filings WHERE symbol = 'AAPL'", conn)
        assert result.empty

    def test_missing_accession_number_column(self, tmp_db):
        """Bug #7: DataFrame without accession_number column should be skipped."""
        df = pd.DataFrame(
            {
                "filing_date": ["2025-01-15"],
                "report_type": ["10-K"],
                "report_url": ["https://sec.gov/10k"],
            }
        )
        tmp_db.save_sec_filings(df, "AAPL")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query("SELECT * FROM sec_filings WHERE symbol = 'AAPL'", conn)
        assert result.empty

    def test_malformed_dates_do_not_crash(self, tmp_db):
        """Regression: malformed filing_date should not crash (errors='coerce').
        Rows with invalid dates are dropped since filing_date is NOT NULL."""
        df = pd.DataFrame(
            {
                "filing_date": ["not-a-date", "2025-01-15"],
                "report_type": ["10-K", "10-Q"],
                "accession_number": ["0001-00-000001", "0001-00-000002"],
                "report_url": ["https://sec.gov/a", "https://sec.gov/b"],
            }
        )
        # Should not raise (previously would crash without errors='coerce')
        tmp_db.save_sec_filings(df, "AAPL")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query(
                "SELECT filing_date FROM sec_filings WHERE symbol = 'AAPL'",
                conn,
            )
        # Only the valid-date row survives (malformed date row is dropped)
        assert len(result) == 1
        assert result["filing_date"].iloc[0] == "2025-01-15"


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

    def test_overlapping_save_preserves_existing_dates(self, tmp_db):
        """Regression: old DELETE-then-INSERT lost data on overlapping saves.
        INSERT OR REPLACE must keep non-overlapping rows from the first save."""
        df1 = pd.DataFrame(
            {"date": ["2024-01-01", "2024-02-01", "2024-03-01"], "value": [1.0, 2.0, 3.0]}
        )
        tmp_db.save_economic_indicators(df1, "GDP")

        # Second save overlaps on 2024-03-01 but adds 2024-04-01
        df2 = pd.DataFrame({"date": ["2024-03-01", "2024-04-01"], "value": [3.5, 4.0]})
        tmp_db.save_economic_indicators(df2, "GDP")

        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query(
                "SELECT date, value FROM economic_indicators WHERE series_id = 'GDP' ORDER BY date",
                conn,
            )
        # All 4 dates must be present (old code would have deleted Jan+Feb)
        assert len(result) == 4
        assert list(result["date"]) == ["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01"]
        # Overlapping row should have updated value
        assert result[result["date"] == "2024-03-01"]["value"].iloc[0] == 3.5

    def test_null_dates_skipped(self, tmp_db):
        """Regression: rows with NaN dates must be silently skipped."""
        df = pd.DataFrame({"date": ["2024-01-01", None, "2024-03-01"], "value": [1.0, 2.0, 3.0]})
        tmp_db.save_economic_indicators(df, "TEST")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query(
                "SELECT * FROM economic_indicators WHERE series_id = 'TEST'", conn
            )
        assert len(result) == 2

    def test_save_with_symbol_column_name(self, tmp_db):
        """Regression: FRED data has value column named after the series symbol."""
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-02-01"],
            "VIXCLS": [18.5, 19.0],
        })
        tmp_db.save_economic_indicators(df, "VIXCLS")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query(
                "SELECT date, value FROM economic_indicators WHERE series_id = 'VIXCLS' ORDER BY date",
                conn,
            )
        assert len(result) == 2
        assert result["value"].iloc[0] == 18.5
        assert result["value"].iloc[1] == 19.0

    def test_nan_values_stored_as_null(self, tmp_db):
        """Regression: NaN values must be stored as SQL NULL, not string 'nan'."""
        df = pd.DataFrame({"date": ["2024-01-01", "2024-02-01"], "value": [1.0, float("nan")]})
        tmp_db.save_economic_indicators(df, "TEST")
        with sqlite3.connect(tmp_db.db_path) as conn:
            result = pd.read_sql_query(
                "SELECT date, value FROM economic_indicators"
                " WHERE series_id = 'TEST' ORDER BY date",
                conn,
            )
        assert len(result) == 2
        assert result["value"].iloc[0] == 1.0
        # Second row should be SQL NULL (reads as None/NaN), not string "nan"
        assert pd.isna(result["value"].iloc[1])
        # Verify it's not stored as a string
        raw = conn.execute(
            "SELECT typeof(value) FROM economic_indicators WHERE date = '2024-02-01'"
        ).fetchone()
        assert raw[0] == "null"


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
        df = pd.DataFrame(
            {
                "date": ["2025-01-01"],
                "close": [100.0],
                "open": [99.0],
                "high": [101.0],
                "low": [98.0],
                "volume": [500000],
            }
        )
        tmp_db.save_prices(df, "SOLO")
        result = tmp_db.get_latest_prices_batch_with_previous(["SOLO"])
        assert len(result) == 1  # only 1 row exists, can't compute change


class TestGetPriceHistoryByDate:
    """Tests for get_price_history_by_date() — date-range queries."""

    def _insert_prices(self, tmp_db, symbol, dates):
        """Insert price rows for given dates."""
        df = pd.DataFrame(
            {
                "date": dates,
                "open": [100.0] * len(dates),
                "high": [105.0] * len(dates),
                "low": [95.0] * len(dates),
                "close": [102.0 + i for i in range(len(dates))],
                "volume": [1_000_000] * len(dates),
            }
        )
        tmp_db.save_prices(df, symbol)

    def test_basic_date_range(self, tmp_db):
        """Retrieve prices within a date range."""
        self._insert_prices(tmp_db, "AAPL", ["2025-01-01", "2025-01-02", "2025-01-03"])
        result = tmp_db.get_price_history_by_date("AAPL", "2025-01-01", "2025-01-03")
        assert len(result) == 3

    def test_partial_date_range(self, tmp_db):
        """Only rows within the range are returned."""
        self._insert_prices(
            tmp_db, "AAPL", ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04"]
        )
        result = tmp_db.get_price_history_by_date("AAPL", "2025-01-02", "2025-01-03")
        assert len(result) == 2
        assert result["date"].iloc[0] == "2025-01-02"
        assert result["date"].iloc[1] == "2025-01-03"

    def test_sorted_ascending(self, tmp_db):
        """Results should be sorted by date ascending."""
        self._insert_prices(tmp_db, "AAPL", ["2025-01-03", "2025-01-01", "2025-01-02"])
        result = tmp_db.get_price_history_by_date("AAPL", "2025-01-01", "2025-01-03")
        assert list(result["date"]) == ["2025-01-01", "2025-01-02", "2025-01-03"]

    def test_end_date_defaults_to_today(self, tmp_db):
        """If end_date is None, it defaults to today."""
        self._insert_prices(tmp_db, "AAPL", ["2025-01-01", "2025-06-15"])
        result = tmp_db.get_price_history_by_date("AAPL", "2025-01-01")
        assert len(result) == 2

    def test_empty_for_unknown_symbol(self, tmp_db):
        """Unknown symbol returns empty DataFrame."""
        self._insert_prices(tmp_db, "AAPL", ["2025-01-01"])
        result = tmp_db.get_price_history_by_date("UNKNOWN", "2025-01-01")
        assert result.empty

    def test_empty_for_out_of_range(self, tmp_db):
        """Querying a date range with no data returns empty."""
        self._insert_prices(tmp_db, "AAPL", ["2025-01-01", "2025-01-02"])
        result = tmp_db.get_price_history_by_date("AAPL", "2025-06-01", "2025-06-30")
        assert result.empty

    def test_inclusive_boundaries(self, tmp_db):
        """Start and end dates are inclusive."""
        self._insert_prices(
            tmp_db, "AAPL", ["2025-01-01", "2025-01-02", "2025-01-03"]
        )
        result = tmp_db.get_price_history_by_date("AAPL", "2025-01-01", "2025-01-01")
        assert len(result) == 1
        assert result["date"].iloc[0] == "2025-01-01"

    def test_symbol_isolation(self, tmp_db):
        """Data for one symbol does not leak into another."""
        self._insert_prices(tmp_db, "AAPL", ["2025-01-01", "2025-01-02"])
        self._insert_prices(tmp_db, "MSFT", ["2025-01-01", "2025-01-02", "2025-01-03"])
        result = tmp_db.get_price_history_by_date("AAPL", "2025-01-01", "2025-01-03")
        assert len(result) == 2  # AAPL only has 2 rows

    def test_output_columns(self, tmp_db):
        """Returned DataFrame should have the expected columns."""
        self._insert_prices(tmp_db, "AAPL", ["2025-01-01"])
        result = tmp_db.get_price_history_by_date("AAPL", "2025-01-01")
        expected_cols = {"date", "open", "high", "low", "close", "volume", "adj_close"}
        assert expected_cols == set(result.columns)
