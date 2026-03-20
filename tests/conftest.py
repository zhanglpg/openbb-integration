"""Shared test fixtures for OpenBB integration tests."""

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Globally disable time.sleep so rate-limit delays and retry backoff
# don't slow down the test suite (~120s → ~5s).
# ---------------------------------------------------------------------------
_real_sleep = time.sleep
time.sleep = lambda _: None

# Mock the openbb module before any src module imports it.
# This is necessary because openbb may not be installed in CI/test environments.
_mock_obb = MagicMock()
sys.modules.setdefault("openbb", _mock_obb)

# Add src to path so imports work like they do in production
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


@pytest.fixture
def tmp_db_path(tmp_path):
    """Return a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
def tmp_db(tmp_db_path):
    """Create a temporary Database instance with schema initialized."""
    with (
        patch("config.DB_PATH", tmp_db_path),
        patch("config.DATA_DIR", tmp_db_path.parent),
        patch("config.CACHE_DIR", tmp_db_path.parent / "cache"),
    ):
        (tmp_db_path.parent / "cache").mkdir(exist_ok=True)
        from database import Database

        return Database(db_path=tmp_db_path)


@pytest.fixture
def sample_price_df():
    """Sample price DataFrame matching OpenBB output format."""
    dates = pd.date_range("2025-01-01", periods=5, freq="B")
    return pd.DataFrame(
        {
            "date": dates,
            "open": [150.0, 151.0, 152.0, 153.0, 154.0],
            "high": [155.0, 156.0, 157.0, 158.0, 159.0],
            "low": [149.0, 150.0, 151.0, 152.0, 153.0],
            "close": [152.0, 153.0, 154.0, 155.0, 156.0],
            "volume": [1000000, 1100000, 1200000, 1300000, 1400000],
        }
    )


@pytest.fixture
def sample_fundamentals_df():
    """Sample fundamentals DataFrame."""
    return pd.DataFrame(
        {
            "market_cap": [2500000000000],
            "pe_ratio": [28.5],
            "pb_ratio": [12.3],
            "debt_to_equity": [1.5],
            "return_on_equity": [0.35],
            "dividend_yield": [0.006],
        }
    )


@pytest.fixture
def sample_filings_df():
    """Sample SEC filings DataFrame."""
    return pd.DataFrame(
        {
            "filing_date": ["2025-01-15", "2025-04-15", "2025-07-15"],
            "report_type": ["10-K", "10-Q", "8-K"],
            "report_url": [
                "https://sec.gov/10k",
                "https://sec.gov/10q",
                "https://sec.gov/8k",
            ],
            "report_date": ["2024-12-31", "2025-03-31", "2025-07-10"],
            "accession_number": ["0001-25-000001", "0001-25-000002", "0001-25-000003"],
            "filing_detail_url": [
                "https://sec.gov/detail/10k",
                "https://sec.gov/detail/10q",
                "https://sec.gov/detail/8k",
            ],
            "primary_doc": ["doc1.htm", "doc2.htm", "doc3.htm"],
            "primary_doc_description": ["Annual Report", "Quarterly Report", "Current Report"],
        }
    )


@pytest.fixture
def sample_economic_df():
    """Sample economic indicator DataFrame."""
    dates = pd.date_range("2024-01-01", periods=12, freq="MS")
    return pd.DataFrame(
        {
            "date": dates,
            "value": [3.5, 3.6, 3.7, 3.8, 3.9, 4.0, 3.9, 3.8, 3.7, 3.6, 3.5, 3.4],
        }
    )


@pytest.fixture
def mock_obb():
    """Mock the OpenBB SDK."""
    with patch("openbb.obb") as mock:
        mock.user.preferences.output_type = "dataframe"
        mock.equity.price.historical.return_value = MagicMock()
        mock.equity.fundamental.metrics.return_value = MagicMock()
        mock.equity.fundamental.income.return_value = MagicMock()
        mock.equity.fundamental.balance.return_value = MagicMock()
        mock.equity.fundamental.cash.return_value = MagicMock()
        mock.equity.fundamental.filings.return_value = MagicMock()
        mock.equity.profile.return_value = MagicMock()
        mock.economy.fred_series.return_value = MagicMock()
        mock.economy.gdp.real.return_value = MagicMock()
        mock.economy.gdp.nominal.return_value = MagicMock()
        mock.economy.cpi.return_value = MagicMock()
        mock.economy.unemployment.return_value = MagicMock()
        mock.economy.interest_rates.return_value = MagicMock()
        yield mock


@pytest.fixture
def tmp_storage(tmp_path):
    """Create a temporary DataStorage instance."""
    from storage import DataStorage

    return DataStorage(base_path=str(tmp_path / "data"))


@pytest.fixture
def tmp_watchlist_file(tmp_path):
    """Create a temporary watchlist file."""
    wl_file = tmp_path / "watchlist.txt"
    wl_file.write_text("# Watchlist\nAAPL\nMSFT\nGOOGL\n")
    return wl_file
