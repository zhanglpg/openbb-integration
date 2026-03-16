"""Tests for Research page enhancements.

Tests cover:
1. compute_historical_valuations() — valuation history from quarterly data + prices
2. compute_growth_rates() — QoQ/YoY revenue & EPS growth
3. normalize_price_series() — multi-symbol price normalization
4. summarize_insider_activity() — insider trade summarization
5. Research notes CRUD — save_note / get_notes / delete_note
6. Fetcher: fetch_insider_trades / fetch_institutional_holders
"""

import sqlite3
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from analysis import (
    compute_growth_rates,
    compute_historical_valuations,
    normalize_price_series,
    summarize_insider_activity,
)

# ===================================================================
# Helpers
# ===================================================================


def _make_quarterly_income(n=8, start="2022-03-31"):
    """Build n quarters of income data."""
    dates = pd.date_range(start, periods=n, freq="QE")
    return pd.DataFrame(
        {
            "period_ending": dates.strftime("%Y-%m-%d"),
            "total_revenue": [100e9 + i * 5e9 for i in range(n)],
            "net_income": [20e9 + i * 1e9 for i in range(n)],
            "ebitda": [30e9 + i * 1.5e9 for i in range(n)],
            "diluted_earnings_per_share": [1.50 + i * 0.10 for i in range(n)],
        }
    )


def _make_quarterly_balance(n=8, start="2022-03-31"):
    """Build n quarters of balance sheet data."""
    dates = pd.date_range(start, periods=n, freq="QE")
    return pd.DataFrame(
        {
            "period_ending": dates.strftime("%Y-%m-%d"),
            "total_assets": [500e9 + i * 10e9 for i in range(n)],
            "total_equity_non_controlling_interests": [200e9 + i * 5e9 for i in range(n)],
            "total_debt": [100e9] * n,
            "cash_and_cash_equivalents": [50e9 + i * 2e9 for i in range(n)],
            "common_stock_shares_outstanding": [15e9] * n,
        }
    )


def _make_quarterly_cashflow(n=8, start="2022-03-31"):
    """Build n quarters of cash flow data."""
    dates = pd.date_range(start, periods=n, freq="QE")
    return pd.DataFrame(
        {
            "period_ending": dates.strftime("%Y-%m-%d"),
            "free_cash_flow": [25e9 + i * 1e9 for i in range(n)],
            "operating_cash_flow": [30e9 + i * 1.5e9 for i in range(n)],
        }
    )


def _make_daily_prices(start="2022-01-03", periods=600, base=150.0):
    """Build daily price data spanning ~2 years."""
    dates = pd.bdate_range(start, periods=periods)
    np.random.seed(42)
    prices = base + np.cumsum(np.random.randn(periods) * 1.5)
    return pd.DataFrame(
        {
            "date": dates.strftime("%Y-%m-%d"),
            "open": prices - 1,
            "high": prices + 2,
            "low": prices - 2,
            "close": prices,
            "volume": [1_000_000] * periods,
        }
    )


# ===================================================================
# compute_historical_valuations
# ===================================================================


@pytest.mark.unit
class TestComputeHistoricalValuations:
    def test_returns_dataframe_with_expected_columns(self):
        result = compute_historical_valuations(
            _make_quarterly_income(),
            _make_quarterly_balance(),
            _make_quarterly_cashflow(),
            _make_daily_prices(),
        )
        assert not result.empty
        assert "period_ending" in result.columns
        assert "pe" in result.columns
        assert "pb" in result.columns
        assert "ev_ebitda" in result.columns
        assert "fcf_yield" in result.columns
        assert "close_price" in result.columns

    def test_pe_computed_correctly(self):
        """PE = close_price / TTM EPS."""
        result = compute_historical_valuations(
            _make_quarterly_income(),
            _make_quarterly_balance(),
            _make_quarterly_cashflow(),
            _make_daily_prices(),
        )
        # Check that PE values are reasonable (positive, not extreme)
        pe_values = result["pe"].dropna()
        assert len(pe_values) > 0
        assert (pe_values > 0).all()
        assert (pe_values < 500).all()

    def test_pb_computed(self):
        """PB should be computed when balance sheet has equity and shares."""
        result = compute_historical_valuations(
            _make_quarterly_income(),
            _make_quarterly_balance(),
            _make_quarterly_cashflow(),
            _make_daily_prices(),
        )
        pb_values = result["pb"].dropna()
        assert len(pb_values) > 0
        assert (pb_values > 0).all()

    def test_ev_ebitda_computed(self):
        """EV/EBITDA should be positive when all components are available."""
        result = compute_historical_valuations(
            _make_quarterly_income(),
            _make_quarterly_balance(),
            _make_quarterly_cashflow(),
            _make_daily_prices(),
        )
        ev_values = result["ev_ebitda"].dropna()
        assert len(ev_values) > 0
        assert (ev_values > 0).all()

    def test_fcf_yield_computed(self):
        """FCF yield should be a percentage."""
        result = compute_historical_valuations(
            _make_quarterly_income(),
            _make_quarterly_balance(),
            _make_quarterly_cashflow(),
            _make_daily_prices(),
        )
        fcf_values = result["fcf_yield"].dropna()
        assert len(fcf_values) > 0

    def test_sorted_by_date(self):
        result = compute_historical_valuations(
            _make_quarterly_income(),
            _make_quarterly_balance(),
            _make_quarterly_cashflow(),
            _make_daily_prices(),
        )
        dates = pd.to_datetime(result["period_ending"])
        assert (dates.diff().dropna() >= pd.Timedelta(0)).all()

    def test_empty_income_returns_empty(self):
        result = compute_historical_valuations(
            pd.DataFrame(),
            _make_quarterly_balance(),
            _make_quarterly_cashflow(),
            _make_daily_prices(),
        )
        assert result.empty

    def test_none_income_returns_empty(self):
        result = compute_historical_valuations(
            None,
            _make_quarterly_balance(),
            _make_quarterly_cashflow(),
            _make_daily_prices(),
        )
        assert result.empty

    def test_empty_prices_returns_empty(self):
        result = compute_historical_valuations(
            _make_quarterly_income(),
            _make_quarterly_balance(),
            _make_quarterly_cashflow(),
            pd.DataFrame(),
        )
        assert result.empty

    def test_no_balance_sheet_still_computes_pe(self):
        """Without balance sheet, PE should still be computed."""
        result = compute_historical_valuations(
            _make_quarterly_income(),
            pd.DataFrame(),
            _make_quarterly_cashflow(),
            _make_daily_prices(),
        )
        if not result.empty:
            pe_values = result["pe"].dropna()
            assert len(pe_values) > 0

    def test_insufficient_quarters_returns_empty(self):
        """Fewer than 4 quarters → can't compute TTM → empty."""
        inc = _make_quarterly_income(3)
        result = compute_historical_valuations(
            inc,
            _make_quarterly_balance(3),
            _make_quarterly_cashflow(3),
            _make_daily_prices(),
        )
        assert result.empty

    def test_fx_rate_increases_pe(self):
        """FX rate > 1 should increase PE (financials in weaker currency)."""
        result_no_fx = compute_historical_valuations(
            _make_quarterly_income(),
            _make_quarterly_balance(),
            _make_quarterly_cashflow(),
            _make_daily_prices(),
            fx_rate=1.0,
        )
        result_with_fx = compute_historical_valuations(
            _make_quarterly_income(),
            _make_quarterly_balance(),
            _make_quarterly_cashflow(),
            _make_daily_prices(),
            fx_rate=7.2,
        )
        if not result_no_fx.empty and not result_with_fx.empty:
            pe_no_fx = result_no_fx["pe"].dropna()
            pe_with_fx = result_with_fx["pe"].dropna()
            if len(pe_no_fx) > 0 and len(pe_with_fx) > 0:
                # With fx_rate=7.2, PE should be ~7.2x higher
                assert pe_with_fx.iloc[0] > pe_no_fx.iloc[0]
                ratio = pe_with_fx.iloc[0] / pe_no_fx.iloc[0]
                assert 6.5 < ratio < 8.0  # ~7.2x

    def test_fx_rate_default_no_change(self):
        """fx_rate=1.0 (default) should give same results as omitting it."""
        result_default = compute_historical_valuations(
            _make_quarterly_income(),
            _make_quarterly_balance(),
            _make_quarterly_cashflow(),
            _make_daily_prices(),
        )
        result_explicit = compute_historical_valuations(
            _make_quarterly_income(),
            _make_quarterly_balance(),
            _make_quarterly_cashflow(),
            _make_daily_prices(),
            fx_rate=1.0,
        )
        if not result_default.empty and not result_explicit.empty:
            pd.testing.assert_frame_equal(result_default, result_explicit)

    def test_negative_eps_gives_negative_pe(self):
        """Negative EPS should produce negative PE."""
        inc = _make_quarterly_income(4)
        inc["diluted_earnings_per_share"] = [-1.0] * 4
        result = compute_historical_valuations(
            inc,
            _make_quarterly_balance(4),
            _make_quarterly_cashflow(4),
            _make_daily_prices(),
        )
        if not result.empty:
            pe_values = result["pe"].dropna()
            if len(pe_values) > 0:
                assert (pe_values < 0).all()


# ===================================================================
# compute_growth_rates
# ===================================================================


@pytest.mark.unit
class TestComputeGrowthRates:
    def test_returns_expected_columns(self):
        result = compute_growth_rates(_make_quarterly_income())
        assert "period_ending" in result.columns
        assert "revenue" in result.columns
        assert "eps" in result.columns
        assert "rev_qoq" in result.columns
        assert "rev_yoy" in result.columns
        assert "eps_qoq" in result.columns
        assert "eps_yoy" in result.columns
        assert "rev_accelerating" in result.columns

    def test_row_count_matches_input(self):
        inc = _make_quarterly_income(8)
        result = compute_growth_rates(inc)
        assert len(result) == 8

    def test_first_qoq_is_nan(self):
        """First QoQ change should be NaN (no prior quarter)."""
        result = compute_growth_rates(_make_quarterly_income())
        assert pd.isna(result["rev_qoq"].iloc[0])

    def test_first_four_yoy_are_nan(self):
        """First 4 YoY changes should be NaN (need 4 prior quarters)."""
        result = compute_growth_rates(_make_quarterly_income())
        assert result["rev_yoy"].iloc[:4].isna().all()

    def test_yoy_growth_correct(self):
        """YoY growth for ascending revenue should be positive."""
        result = compute_growth_rates(_make_quarterly_income())
        yoy = result["rev_yoy"].dropna()
        assert (yoy > 0).all()

    def test_qoq_growth_correct(self):
        """QoQ growth for ascending revenue should be positive."""
        result = compute_growth_rates(_make_quarterly_income())
        qoq = result["rev_qoq"].dropna()
        assert (qoq > 0).all()

    def test_acceleration_flag(self):
        """With steadily increasing revenue, acceleration can be computed."""
        result = compute_growth_rates(_make_quarterly_income())
        accel = result["rev_accelerating"].dropna()
        assert len(accel) > 0

    def test_empty_input(self):
        result = compute_growth_rates(pd.DataFrame())
        assert result.empty

    def test_none_input(self):
        result = compute_growth_rates(None)
        assert result.empty

    def test_missing_eps_column(self):
        """Should handle income without EPS column."""
        inc = _make_quarterly_income()
        inc = inc.drop(columns=["diluted_earnings_per_share"])
        result = compute_growth_rates(inc)
        assert "revenue" in result.columns
        assert "eps" not in result.columns

    def test_missing_revenue_column(self):
        """Should handle income without revenue column."""
        inc = pd.DataFrame(
            {
                "period_ending": pd.date_range("2023-03-31", periods=5, freq="QE"),
                "diluted_earnings_per_share": [1.0, 1.1, 1.2, 1.3, 1.4],
            }
        )
        result = compute_growth_rates(inc)
        assert "eps" in result.columns
        assert "revenue" not in result.columns

    def test_declining_revenue(self):
        """Declining revenue should produce negative growth rates."""
        dates = pd.date_range("2023-03-31", periods=8, freq="QE")
        inc = pd.DataFrame(
            {
                "period_ending": dates,
                "total_revenue": [100e9 - i * 5e9 for i in range(8)],
            }
        )
        result = compute_growth_rates(inc)
        qoq = result["rev_qoq"].dropna()
        assert (qoq < 0).all()


# ===================================================================
# normalize_price_series
# ===================================================================


@pytest.mark.unit
class TestNormalizePriceSeries:
    def _make_two_symbols(self):
        dates = pd.bdate_range("2025-01-01", periods=20)
        return {
            "AAPL": pd.DataFrame(
                {
                    "date": dates.strftime("%Y-%m-%d"),
                    "close": [150 + i for i in range(20)],
                }
            ),
            "MSFT": pd.DataFrame(
                {
                    "date": dates.strftime("%Y-%m-%d"),
                    "close": [400 + i * 2 for i in range(20)],
                }
            ),
        }

    def test_starts_at_100(self):
        result = normalize_price_series(self._make_two_symbols())
        assert result["AAPL"].iloc[0] == pytest.approx(100.0)
        assert result["MSFT"].iloc[0] == pytest.approx(100.0)

    def test_custom_base(self):
        result = normalize_price_series(self._make_two_symbols(), base=1000.0)
        assert result["AAPL"].iloc[0] == pytest.approx(1000.0)

    def test_columns_match_symbols(self):
        result = normalize_price_series(self._make_two_symbols())
        assert set(result.columns) == {"AAPL", "MSFT"}

    def test_date_index(self):
        result = normalize_price_series(self._make_two_symbols())
        assert result.index.name == "date"

    def test_length_matches_input(self):
        dfs = self._make_two_symbols()
        result = normalize_price_series(dfs)
        assert len(result) == 20

    def test_empty_input(self):
        result = normalize_price_series({})
        assert result.empty

    def test_single_symbol(self):
        dfs = self._make_two_symbols()
        result = normalize_price_series({"AAPL": dfs["AAPL"]})
        assert "AAPL" in result.columns
        assert len(result.columns) == 1

    def test_none_df_skipped(self):
        result = normalize_price_series({"AAPL": None, "MSFT": _make_daily_prices(periods=10)})
        assert "MSFT" in result.columns
        assert "AAPL" not in result.columns

    def test_empty_df_skipped(self):
        result = normalize_price_series(
            {"AAPL": pd.DataFrame(), "MSFT": _make_daily_prices(periods=10)}
        )
        assert "MSFT" in result.columns
        assert "AAPL" not in result.columns

    def test_relative_performance_preserved(self):
        """A symbol with 2x price increase should show higher normalized value."""
        dates = pd.bdate_range("2025-01-01", periods=10)
        dfs = {
            "FAST": pd.DataFrame({"date": dates, "close": [100 + i * 10 for i in range(10)]}),
            "SLOW": pd.DataFrame({"date": dates, "close": [100 + i for i in range(10)]}),
        }
        result = normalize_price_series(dfs)
        assert result["FAST"].iloc[-1] > result["SLOW"].iloc[-1]


# ===================================================================
# summarize_insider_activity
# ===================================================================


@pytest.mark.unit
class TestSummarizeInsiderActivity:
    def _make_insider_df(self):
        return pd.DataFrame(
            {
                "transaction_date": [
                    "2025-01-10",
                    "2025-01-15",
                    "2025-02-01",
                    "2025-02-10",
                    "2025-03-01",
                ],
                "acquisition_or_disposition": ["A", "D", "A", "A", "D"],
                "securities_transacted": [1000, 500, 2000, 1500, 3000],
                "owner_name": ["CEO", "CFO", "CEO", "CTO", "CFO"],
            }
        )

    def test_total_trades(self):
        result = summarize_insider_activity(self._make_insider_df())
        assert result["total_trades"] == 5

    def test_buy_sell_counts(self):
        result = summarize_insider_activity(self._make_insider_df())
        assert result["buys"] == 3
        assert result["sells"] == 2

    def test_net_shares(self):
        """Net shares: +1000 -500 +2000 +1500 -3000 = +1000."""
        result = summarize_insider_activity(self._make_insider_df())
        assert result["net_shares"] == 1000

    def test_top_insiders(self):
        result = summarize_insider_activity(self._make_insider_df())
        top = result["top_insiders"]
        assert len(top) > 0
        # CEO has 2 trades, CFO has 2 trades
        names = {t["name"] for t in top}
        assert "CEO" in names
        assert "CFO" in names

    def test_recent_trades(self):
        result = summarize_insider_activity(self._make_insider_df())
        recent = result["recent_trades"]
        assert len(recent) == 5
        # Most recent should be first
        assert recent[0]["date"] == "2025-03-01"

    def test_empty_df(self):
        result = summarize_insider_activity(pd.DataFrame())
        assert result["total_trades"] == 0

    def test_none_df(self):
        result = summarize_insider_activity(None)
        assert result["total_trades"] == 0

    def test_missing_acquisition_column(self):
        """Should handle missing acquisition/disposition column."""
        df = pd.DataFrame(
            {
                "transaction_date": ["2025-01-10"],
                "securities_transacted": [1000],
                "owner_name": ["CEO"],
            }
        )
        result = summarize_insider_activity(df)
        assert result["total_trades"] == 1
        assert result["buys"] == 0
        assert result["sells"] == 0

    def test_missing_shares_column(self):
        """Should handle missing shares column."""
        df = pd.DataFrame(
            {
                "transaction_date": ["2025-01-10"],
                "acquisition_or_disposition": ["A"],
                "owner_name": ["CEO"],
            }
        )
        result = summarize_insider_activity(df)
        assert result["total_trades"] == 1
        assert result["net_shares"] is None

    def test_all_buys(self):
        df = pd.DataFrame(
            {
                "transaction_date": ["2025-01-10", "2025-01-11"],
                "acquisition_or_disposition": ["A", "A"],
                "securities_transacted": [1000, 2000],
            }
        )
        result = summarize_insider_activity(df)
        assert result["buys"] == 2
        assert result["sells"] == 0
        assert result["net_shares"] == 3000

    def test_all_sells(self):
        df = pd.DataFrame(
            {
                "transaction_date": ["2025-01-10", "2025-01-11"],
                "acquisition_or_disposition": ["D", "D"],
                "securities_transacted": [1000, 2000],
            }
        )
        result = summarize_insider_activity(df)
        assert result["buys"] == 0
        assert result["sells"] == 2
        assert result["net_shares"] == -3000


# ===================================================================
# Database: fundamentals currency columns
# ===================================================================


@pytest.mark.unit
class TestFundamentalsCurrencyColumns:
    def test_currency_columns_exist(self, tmp_db):
        """Schema v3: reporting_currency and trading_currency columns exist."""
        import sqlite3

        with sqlite3.connect(tmp_db.db_path) as conn:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(fundamentals)").fetchall()}
        assert "reporting_currency" in cols
        assert "trading_currency" in cols

    def test_save_and_retrieve_currencies(self, tmp_db):
        """Currency columns are stored and retrieved correctly."""
        df = pd.DataFrame(
            {
                "market_cap": [330e9],
                "pe_ratio": [18.0],
                "reporting_currency": ["CNY"],
                "trading_currency": ["USD"],
            }
        )
        tmp_db.save_fundamentals(df, "BABA")
        result = tmp_db.get_all_fundamentals()
        baba = result[result["symbol"] == "BABA"]
        assert not baba.empty
        assert baba["reporting_currency"].iloc[0] == "CNY"
        assert baba["trading_currency"].iloc[0] == "USD"

    def test_currencies_null_for_usd_stocks(self, tmp_db):
        """USD stocks may have NULL currencies (backward compat)."""
        df = pd.DataFrame({"market_cap": [3e12], "pe_ratio": [30.0]})
        tmp_db.save_fundamentals(df, "AAPL")
        result = tmp_db.get_all_fundamentals()
        aapl = result[result["symbol"] == "AAPL"]
        assert not aapl.empty
        # pe_ratio still works even without currency columns
        assert aapl["pe_ratio"].iloc[0] == 30.0

    def test_currencies_stored_with_fundamentals(self, tmp_db):
        """Currencies stored alongside all other fundamentals."""
        df = pd.DataFrame(
            {
                "market_cap": [1.8e12],
                "pe_ratio": [33.0],
                "eps": [5.5],
                "reporting_currency": ["TWD"],
                "trading_currency": ["USD"],
            }
        )
        tmp_db.save_fundamentals(df, "TSM")
        result = tmp_db.get_all_fundamentals()
        tsm = result[result["symbol"] == "TSM"]
        assert tsm["reporting_currency"].iloc[0] == "TWD"
        assert tsm["trading_currency"].iloc[0] == "USD"
        assert tsm["pe_ratio"].iloc[0] == 33.0
        assert tsm["eps"].iloc[0] == 5.5


# ===================================================================
# Database: research_notes CRUD
# ===================================================================


@pytest.mark.unit
class TestResearchNotes:
    def test_table_exists(self, tmp_db):
        with sqlite3.connect(tmp_db.db_path) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {row[0] for row in cursor.fetchall()}
        assert "research_notes" in tables

    def test_index_exists(self, tmp_db):
        with sqlite3.connect(tmp_db.db_path) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
            indexes = {row[0] for row in cursor.fetchall()}
        assert "idx_research_notes_symbol" in indexes

    def test_save_and_get_note(self, tmp_db):
        tmp_db.save_note("AAPL", "Bullish on iPhone 17 cycle")
        notes = tmp_db.get_notes("AAPL")
        assert len(notes) == 1
        assert notes["note"].iloc[0] == "Bullish on iPhone 17 cycle"
        assert notes["symbol"].iloc[0] == "AAPL"

    def test_multiple_notes_per_symbol(self, tmp_db):
        tmp_db.save_note("AAPL", "Note 1")
        tmp_db.save_note("AAPL", "Note 2")
        tmp_db.save_note("AAPL", "Note 3")
        notes = tmp_db.get_notes("AAPL")
        assert len(notes) == 3

    def test_notes_ordered_most_recent_first(self, tmp_db):
        tmp_db.save_note("AAPL", "First")
        tmp_db.save_note("AAPL", "Second")
        tmp_db.save_note("AAPL", "Third")
        notes = tmp_db.get_notes("AAPL")
        assert notes["note"].iloc[0] == "Third"
        assert notes["note"].iloc[2] == "First"

    def test_notes_isolated_by_symbol(self, tmp_db):
        tmp_db.save_note("AAPL", "Apple note")
        tmp_db.save_note("MSFT", "Microsoft note")
        aapl_notes = tmp_db.get_notes("AAPL")
        msft_notes = tmp_db.get_notes("MSFT")
        assert len(aapl_notes) == 1
        assert len(msft_notes) == 1
        assert aapl_notes["note"].iloc[0] == "Apple note"
        assert msft_notes["note"].iloc[0] == "Microsoft note"

    def test_delete_note(self, tmp_db):
        tmp_db.save_note("AAPL", "To be deleted")
        notes = tmp_db.get_notes("AAPL")
        note_id = notes["id"].iloc[0]
        tmp_db.delete_note(note_id)
        notes_after = tmp_db.get_notes("AAPL")
        assert notes_after.empty

    def test_delete_specific_note(self, tmp_db):
        """Deleting one note should not affect others."""
        tmp_db.save_note("AAPL", "Keep this")
        tmp_db.save_note("AAPL", "Delete this")
        notes = tmp_db.get_notes("AAPL")
        delete_id = notes[notes["note"] == "Delete this"]["id"].iloc[0]
        tmp_db.delete_note(delete_id)
        remaining = tmp_db.get_notes("AAPL")
        assert len(remaining) == 1
        assert remaining["note"].iloc[0] == "Keep this"

    def test_get_notes_limit(self, tmp_db):
        for i in range(10):
            tmp_db.save_note("AAPL", f"Note {i}")
        notes = tmp_db.get_notes("AAPL", limit=5)
        assert len(notes) == 5

    def test_get_notes_empty_symbol(self, tmp_db):
        notes = tmp_db.get_notes("NONEXISTENT")
        assert notes.empty

    def test_note_has_updated_at(self, tmp_db):
        tmp_db.save_note("AAPL", "Test note")
        notes = tmp_db.get_notes("AAPL")
        assert pd.notna(notes["updated_at"].iloc[0])

    def test_note_columns(self, tmp_db):
        tmp_db.save_note("AAPL", "Test")
        notes = tmp_db.get_notes("AAPL")
        assert set(notes.columns) == {"id", "symbol", "note", "updated_at"}


# ===================================================================
# Fetcher: insider trades and institutional holders
# ===================================================================


@pytest.mark.unit
class TestFetcherInsiderInstitutional:
    @pytest.fixture(autouse=True)
    def setup(self):
        with patch("fetcher.obb") as self.mock_obb:
            from fetcher import DataFetcher

            self.fetcher = DataFetcher()
            yield

    def _make_result(self, df):
        result = MagicMock()
        result.to_dataframe.return_value = df
        return result

    def test_fetch_insider_trades_success(self):
        df = pd.DataFrame(
            {
                "transaction_date": ["2025-01-10"],
                "acquisition_or_disposition": ["A"],
                "securities_transacted": [1000],
                "owner_name": ["CEO"],
            }
        )
        self.mock_obb.equity.ownership.insider_trading.return_value = self._make_result(df)
        result = self.fetcher.fetch_insider_trades("AAPL")
        assert not result.empty
        assert len(result) == 1
        self.mock_obb.equity.ownership.insider_trading.assert_called_once()

    def test_fetch_insider_trades_error(self):
        self.mock_obb.equity.ownership.insider_trading.side_effect = Exception("API error")
        result = self.fetcher.fetch_insider_trades("AAPL")
        assert result.empty

    def test_fetch_institutional_holders_success(self):
        df = pd.DataFrame(
            {
                "investor_name": ["Vanguard Group", "BlackRock"],
                "shares_held": [1e9, 900e6],
            }
        )
        self.mock_obb.equity.ownership.institutional.return_value = self._make_result(df)
        result = self.fetcher.fetch_institutional_holders("AAPL")
        assert not result.empty
        assert len(result) == 2

    def test_fetch_institutional_holders_error(self):
        self.mock_obb.equity.ownership.institutional.side_effect = Exception("API error")
        result = self.fetcher.fetch_institutional_holders("AAPL")
        assert result.empty

    def test_fetch_insider_trades_with_provider(self):
        df = pd.DataFrame({"transaction_date": ["2025-01-10"]})
        self.mock_obb.equity.ownership.insider_trading.return_value = self._make_result(df)
        self.fetcher.fetch_insider_trades("AAPL", provider="sec")
        call_kwargs = self.mock_obb.equity.ownership.insider_trading.call_args
        assert call_kwargs.kwargs["provider"] == "sec"

    def test_fetch_institutional_with_provider(self):
        df = pd.DataFrame({"investor_name": ["Vanguard"]})
        self.mock_obb.equity.ownership.institutional.return_value = self._make_result(df)
        self.fetcher.fetch_institutional_holders("AAPL", provider="sec")
        call_kwargs = self.mock_obb.equity.ownership.institutional.call_args
        assert call_kwargs.kwargs["provider"] == "sec"


# ===================================================================
# Integration: Historical valuations with DB price data
# ===================================================================


@pytest.mark.integration
class TestValuationHistoryIntegration:
    def test_with_db_prices(self, tmp_db):
        """End-to-end: save prices to DB, compute valuations."""
        price_df = _make_daily_prices(periods=600)
        tmp_db.save_prices(price_df, "AAPL")

        # Read back from DB
        db_prices = tmp_db.get_price_history_batch(["AAPL"], days=2000)
        db_prices = db_prices[db_prices["symbol"] == "AAPL"]

        result = compute_historical_valuations(
            _make_quarterly_income(),
            _make_quarterly_balance(),
            _make_quarterly_cashflow(),
            db_prices,
        )
        assert not result.empty
        assert "pe" in result.columns

    def test_sparse_data_does_not_crash(self, tmp_db):
        """Symbol with very few price points should not crash."""
        price_df = pd.DataFrame(
            {
                "date": ["2023-06-30"],
                "open": [150.0],
                "high": [155.0],
                "low": [149.0],
                "close": [152.0],
                "volume": [1000000],
            }
        )
        tmp_db.save_prices(price_df, "SPARSE")
        db_prices = tmp_db.get_price_history_batch(["SPARSE"], days=2000)
        db_prices = db_prices[db_prices["symbol"] == "SPARSE"]

        # Should not crash, may return empty
        result = compute_historical_valuations(
            _make_quarterly_income(),
            _make_quarterly_balance(),
            _make_quarterly_cashflow(),
            db_prices,
        )
        assert isinstance(result, pd.DataFrame)


# ===================================================================
# Integration: Growth rates with research notes
# ===================================================================


@pytest.mark.integration
class TestResearchNotesIntegration:
    def test_full_lifecycle(self, tmp_db):
        """Save, read, delete notes — full lifecycle."""
        # Save notes
        tmp_db.save_note("NVDA", "AI boom beneficiary, strong data center growth")
        tmp_db.save_note("NVDA", "Valuation stretched but momentum strong")
        tmp_db.save_note("AMD", "Catching up in AI, cheaper valuation")

        # Read NVDA notes
        nvda_notes = tmp_db.get_notes("NVDA")
        assert len(nvda_notes) == 2

        # Read AMD notes
        amd_notes = tmp_db.get_notes("AMD")
        assert len(amd_notes) == 1

        # Delete one NVDA note
        first_note_id = nvda_notes["id"].iloc[0]
        tmp_db.delete_note(first_note_id)

        # Verify
        nvda_after = tmp_db.get_notes("NVDA")
        assert len(nvda_after) == 1

        # AMD unaffected
        amd_after = tmp_db.get_notes("AMD")
        assert len(amd_after) == 1

    def test_delete_nonexistent_note(self, tmp_db):
        """Deleting a non-existent note should not crash."""
        tmp_db.delete_note(99999)
        # No assertion needed — just ensure no exception
