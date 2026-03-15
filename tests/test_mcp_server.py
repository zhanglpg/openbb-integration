"""Tests for MCP server tools (src/mcp_server.py).

Unit tests mock the database layer; integration tests use a real SQLite DB.
"""

import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Ensure src/ is importable (conftest.py also does this)
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Mock fastmcp before importing mcp_server so the module loads without the package
_mock_fastmcp = MagicMock()
_real_decorators = {}


def _passthrough_decorator(*args, **kwargs):
    """Return the function unchanged, capturing registration."""
    if args and callable(args[0]):
        return args[0]
    return lambda fn: fn


_mock_fastmcp.FastMCP.return_value.tool = _passthrough_decorator
_mock_fastmcp.FastMCP.return_value.resource = _passthrough_decorator
sys.modules.setdefault("fastmcp", _mock_fastmcp)


# ---------------------------------------------------------------------------
# Import after mocks are in place
# ---------------------------------------------------------------------------

# We need to import the *functions* from mcp_server, but the module creates
# a Database() at import time.  Patch DB_PATH + Database so import succeeds.
_tmp_db_path = Path("/tmp/_mcp_test_placeholder.db")
with (
    patch("config.DB_PATH", _tmp_db_path),
    patch("config.DATA_DIR", _tmp_db_path.parent),
    patch("config.CACHE_DIR", _tmp_db_path.parent / "cache"),
    patch("database.Database.__init__", lambda self, **kw: None),
):
    import mcp_server


# ===================================================================
# Fixtures
# ===================================================================


@pytest.fixture
def mcp_db(tmp_db):
    """Patch the module-level db and DB_PATH used by mcp_server functions."""
    with patch.object(mcp_server, "db", tmp_db), patch.object(
        mcp_server, "DB_PATH", tmp_db.db_path
    ):
        yield tmp_db


def _seed_prices(db, symbol, dates_and_closes):
    """Insert price rows into the database."""
    for date, close in dates_and_closes:
        df = pd.DataFrame(
            {
                "date": [date],
                "open": [close - 1],
                "high": [close + 1],
                "low": [close - 2],
                "close": [close],
                "volume": [1_000_000],
            }
        )
        db.save_prices(df, symbol)


def _seed_fundamentals(db, symbol, snapshot_date="2025-06-01"):
    """Insert a fundamentals row directly into SQLite for date control."""
    with sqlite3.connect(db.db_path) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO fundamentals
               (symbol, snapshot_date, market_cap, pe_ratio, pb_ratio,
                debt_to_equity, return_on_equity, dividend_yield)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (symbol, snapshot_date, 2_500_000_000_000, 28.5, 12.3, 1.5, 0.35, 0.006),
        )


def _seed_sec_filings(db, symbol):
    """Insert SEC filing rows."""
    df = pd.DataFrame(
        {
            "filing_date": ["2025-01-15", "2025-04-15"],
            "report_type": ["10-K", "10-Q"],
            "report_url": ["https://sec.gov/10k", "https://sec.gov/10q"],
            "report_date": ["2024-12-31", "2025-03-31"],
            "accession_number": ["0001-25-000001", "0001-25-000002"],
            "filing_detail_url": [
                "https://sec.gov/detail/10k",
                "https://sec.gov/detail/10q",
            ],
            "primary_doc": ["doc1.htm", "doc2.htm"],
            "primary_doc_description": ["Annual Report", "Quarterly Report"],
        }
    )
    db.save_sec_filings(df, symbol)


def _seed_economic(db, series_id, dates_and_values):
    """Insert economic indicator rows."""
    df = pd.DataFrame(
        {"date": [d for d, _ in dates_and_values], "value": [v for _, v in dates_and_values]}
    )
    db.save_economic_indicators(df, series_id)


# ===================================================================
# Unit tests — get_watchlist (no DB needed)
# ===================================================================


@pytest.mark.unit
class TestGetWatchlist:
    def test_returns_watchlist_dict(self):
        result = mcp_server.get_watchlist()
        assert isinstance(result, dict)
        assert "tech" in result
        assert "AAPL" in result["tech"]

    def test_all_categories_present(self):
        result = mcp_server.get_watchlist()
        for cat in ("tech", "china", "semiconductors", "etfs"):
            assert cat in result

    def test_symbols_are_lists(self):
        for symbols in mcp_server.get_watchlist().values():
            assert isinstance(symbols, list)
            assert all(isinstance(s, str) for s in symbols)


# ===================================================================
# Unit tests — get_portfolio_overview
# ===================================================================


@pytest.mark.unit
class TestGetPortfolioOverview:
    def test_empty_db_returns_empty_list(self, mcp_db):
        assert mcp_server.get_portfolio_overview() == []

    def test_single_symbol_no_change(self, mcp_db):
        """A symbol with only one price row should have change_pct=None."""
        _seed_prices(mcp_db, "AAPL", [("2025-06-01", 150.0)])
        result = mcp_server.get_portfolio_overview()
        aapl = [r for r in result if r["symbol"] == "AAPL"]
        assert len(aapl) == 1
        assert aapl[0]["price"] == 150.0
        assert aapl[0]["change_pct"] is None

    def test_two_days_computes_change(self, mcp_db):
        _seed_prices(mcp_db, "AAPL", [("2025-06-01", 100.0), ("2025-06-02", 105.0)])
        result = mcp_server.get_portfolio_overview()
        aapl = [r for r in result if r["symbol"] == "AAPL"][0]
        assert aapl["change_pct"] == 5.0

    def test_sector_mapping(self, mcp_db):
        _seed_prices(mcp_db, "AAPL", [("2025-06-01", 150.0)])
        result = mcp_server.get_portfolio_overview()
        aapl = [r for r in result if r["symbol"] == "AAPL"][0]
        assert aapl["sector"] == "tech"

    def test_unknown_symbol_sector(self, mcp_db):
        """Symbols not in the watchlist get sector='unknown'."""
        _seed_prices(mcp_db, "XYZ", [("2025-06-01", 50.0)])
        # XYZ is not in ALL_SYMBOLS, so get_latest_prices_batch_with_previous
        # won't fetch it unless we override ALL_SYMBOLS. Test via direct call.
        with patch.object(mcp_server, "ALL_SYMBOLS", ["XYZ"]):
            result = mcp_server.get_portfolio_overview()
        xyz = [r for r in result if r["symbol"] == "XYZ"]
        assert len(xyz) == 1
        assert xyz[0]["sector"] == "unknown"


# ===================================================================
# Unit tests — get_price_history
# ===================================================================


@pytest.mark.unit
class TestGetPriceHistory:
    def test_empty_returns_empty_list(self, mcp_db):
        assert mcp_server.get_price_history("AAPL") == []

    def test_returns_records(self, mcp_db):
        _seed_prices(mcp_db, "AAPL", [("2025-06-01", 150.0), ("2025-06-02", 152.0)])
        result = mcp_server.get_price_history("AAPL")
        assert len(result) == 2
        assert "close" in result[0]
        assert "symbol" in result[0]

    def test_drops_internal_columns(self, mcp_db):
        _seed_prices(mcp_db, "AAPL", [("2025-06-01", 150.0)])
        result = mcp_server.get_price_history("AAPL")
        assert "id" not in result[0]
        assert "fetched_at" not in result[0]

    def test_case_insensitive_symbol(self, mcp_db):
        _seed_prices(mcp_db, "AAPL", [("2025-06-01", 150.0)])
        result = mcp_server.get_price_history("aapl")
        assert len(result) == 1

    def test_days_parameter(self, mcp_db):
        dates = [(f"2025-06-{i:02d}", 150.0 + i) for i in range(1, 11)]
        _seed_prices(mcp_db, "AAPL", dates)
        result = mcp_server.get_price_history("AAPL", days=3)
        assert len(result) == 3


# ===================================================================
# Unit tests — get_fundamentals
# ===================================================================


@pytest.mark.unit
class TestGetFundamentals:
    def test_no_data_returns_empty_dict(self, mcp_db):
        assert mcp_server.get_fundamentals("AAPL") == {}

    def test_returns_fundamentals(self, mcp_db):
        _seed_fundamentals(mcp_db, "AAPL")
        result = mcp_server.get_fundamentals("AAPL")
        assert result["symbol"] == "AAPL"
        assert "market_cap" in result

    def test_drops_internal_columns(self, mcp_db):
        _seed_fundamentals(mcp_db, "AAPL")
        result = mcp_server.get_fundamentals("AAPL")
        assert "id" not in result
        assert "fetched_at" not in result

    def test_case_insensitive(self, mcp_db):
        _seed_fundamentals(mcp_db, "AAPL")
        result = mcp_server.get_fundamentals("aapl")
        assert result["symbol"] == "AAPL"

    def test_returns_latest_snapshot(self, mcp_db):
        _seed_fundamentals(mcp_db, "AAPL", snapshot_date="2025-01-01")
        _seed_fundamentals(mcp_db, "AAPL", snapshot_date="2025-06-01")
        result = mcp_server.get_fundamentals("AAPL")
        assert result["snapshot_date"] == "2025-06-01"


# ===================================================================
# Unit tests — get_sec_filings
# ===================================================================


@pytest.mark.unit
class TestGetSecFilings:
    def test_no_data_returns_empty_list(self, mcp_db):
        assert mcp_server.get_sec_filings("AAPL") == []

    def test_returns_filings(self, mcp_db):
        _seed_sec_filings(mcp_db, "AAPL")
        result = mcp_server.get_sec_filings("AAPL")
        assert len(result) == 2
        assert result[0]["report_type"] in ("10-K", "10-Q")

    def test_drops_internal_columns(self, mcp_db):
        _seed_sec_filings(mcp_db, "AAPL")
        result = mcp_server.get_sec_filings("AAPL")
        assert "id" not in result[0]
        assert "fetched_at" not in result[0]

    def test_limit_parameter(self, mcp_db):
        _seed_sec_filings(mcp_db, "AAPL")
        result = mcp_server.get_sec_filings("AAPL", limit=1)
        assert len(result) == 1

    def test_ordered_by_date_desc(self, mcp_db):
        _seed_sec_filings(mcp_db, "AAPL")
        result = mcp_server.get_sec_filings("AAPL")
        dates = [r["filing_date"] for r in result]
        assert dates == sorted(dates, reverse=True)


# ===================================================================
# Unit tests — get_economic_indicators
# ===================================================================


@pytest.mark.unit
class TestGetEconomicIndicators:
    def test_empty_returns_empty_list(self, mcp_db):
        assert mcp_server.get_economic_indicators() == []

    def test_returns_indicators(self, mcp_db):
        _seed_economic(mcp_db, "FEDFUNDS", [("2025-01-01", 5.25), ("2025-02-01", 5.50)])
        result = mcp_server.get_economic_indicators()
        assert len(result) == 1  # latest per series
        assert result[0]["series_id"] == "FEDFUNDS"
        assert result[0]["value"] == 5.50

    def test_filter_by_series_ids(self, mcp_db):
        _seed_economic(mcp_db, "FEDFUNDS", [("2025-01-01", 5.25)])
        _seed_economic(mcp_db, "UNRATE", [("2025-01-01", 3.7)])
        result = mcp_server.get_economic_indicators(series_ids=["UNRATE"])
        assert len(result) == 1
        assert result[0]["series_id"] == "UNRATE"

    def test_all_series_returned_when_no_filter(self, mcp_db):
        _seed_economic(mcp_db, "FEDFUNDS", [("2025-01-01", 5.25)])
        _seed_economic(mcp_db, "UNRATE", [("2025-01-01", 3.7)])
        result = mcp_server.get_economic_indicators()
        assert len(result) == 2


# ===================================================================
# Integration tests — full roundtrip through real SQLite
# ===================================================================


@pytest.mark.integration
class TestMCPIntegration:
    """End-to-end tests: seed DB → call MCP tool → verify output."""

    def test_portfolio_overview_multiple_symbols(self, mcp_db):
        _seed_prices(mcp_db, "AAPL", [("2025-06-01", 100.0), ("2025-06-02", 110.0)])
        _seed_prices(mcp_db, "MSFT", [("2025-06-01", 400.0), ("2025-06-02", 404.0)])
        result = mcp_server.get_portfolio_overview()
        symbols = {r["symbol"] for r in result}
        assert "AAPL" in symbols
        assert "MSFT" in symbols
        aapl = [r for r in result if r["symbol"] == "AAPL"][0]
        assert aapl["change_pct"] == 10.0
        msft = [r for r in result if r["symbol"] == "MSFT"][0]
        assert msft["change_pct"] == 1.0

    def test_price_history_roundtrip(self, mcp_db):
        dates = [(f"2025-06-{i:02d}", 150.0 + i) for i in range(1, 6)]
        _seed_prices(mcp_db, "GOOGL", dates)
        result = mcp_server.get_price_history("GOOGL", days=5)
        assert len(result) == 5
        closes = [r["close"] for r in result]
        # Should be ordered by date DESC (from get_latest_prices)
        assert closes == sorted(closes, reverse=True)

    def test_fundamentals_roundtrip(self, mcp_db):
        _seed_fundamentals(mcp_db, "META")
        result = mcp_server.get_fundamentals("META")
        assert result["symbol"] == "META"
        assert result["market_cap"] == 2_500_000_000_000

    def test_sec_filings_roundtrip(self, mcp_db):
        _seed_sec_filings(mcp_db, "NVDA")
        result = mcp_server.get_sec_filings("NVDA")
        assert len(result) == 2
        types = {r["report_type"] for r in result}
        assert types == {"10-K", "10-Q"}

    def test_economic_roundtrip(self, mcp_db):
        _seed_economic(mcp_db, "GDP", [("2025-01-01", 28000.0), ("2025-04-01", 28500.0)])
        _seed_economic(mcp_db, "CPIAUCSL", [("2025-01-01", 310.5)])
        result = mcp_server.get_economic_indicators()
        assert len(result) == 2
        gdp = [r for r in result if r["series_id"] == "GDP"][0]
        assert gdp["value"] == 28500.0  # latest

    def test_cross_tool_consistency(self, mcp_db):
        """Portfolio overview and price history should agree on latest price."""
        _seed_prices(mcp_db, "AAPL", [("2025-06-01", 100.0), ("2025-06-02", 105.0)])
        overview = mcp_server.get_portfolio_overview()
        history = mcp_server.get_price_history("AAPL", days=1)
        aapl_overview = [r for r in overview if r["symbol"] == "AAPL"][0]
        assert aapl_overview["price"] == history[0]["close"]

    def test_watchlist_matches_config(self, mcp_db):
        """get_watchlist should return the same dict used by ALL_SYMBOLS."""
        wl = mcp_server.get_watchlist()
        all_from_wl = sorted(set(s for syms in wl.values() for s in syms))
        assert all_from_wl == mcp_server.ALL_SYMBOLS
