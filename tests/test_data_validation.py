"""Data validation tests — verify financial data against SEC filings.

These tests fetch LIVE data from OpenBB/yfinance and compare against
known figures from official 10-K filings and press releases.  They
require network access and are marked ``validation`` so they can be
run separately:

    pytest tests/test_data_validation.py -xvs
    pytest -m validation

Reference values sourced from:
  - AAPL: 10-K filed Oct 2024 (FY ended Sep 28, 2024)
  - GOOGL: 10-K filed Feb 2025 (FY ended Dec 31, 2024)
  - BABA: Press release May 2025 (FY ended Mar 31, 2025)
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Validation tests need the REAL openbb module, not the mock from conftest.py.
# Remove any mock and force-reimport openbb and fetcher with the real SDK.
if "openbb" in sys.modules and hasattr(sys.modules["openbb"], "_mock_name"):
    del sys.modules["openbb"]

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Force reimport fetcher to pick up real openbb
for mod_name in list(sys.modules):
    if mod_name in ("fetcher", "retry"):
        del sys.modules[mod_name]

from fetcher import DataFetcher  # noqa: E402

# Shared fetcher instance — instantiated once per module
_fetcher = DataFetcher()


# ===================================================================
# Helpers
# ===================================================================


def _get_annual_row(df: pd.DataFrame, fiscal_year_end: str) -> pd.Series | None:
    """Find the row matching a fiscal year end date (YYYY-MM-DD)."""
    if df.empty or "period_ending" not in df.columns:
        return None
    df = df.copy()
    df["period_ending"] = pd.to_datetime(df["period_ending"]).dt.strftime("%Y-%m-%d")
    match = df[df["period_ending"] == fiscal_year_end]
    if match.empty:
        # Try matching just the year-month (some providers round dates)
        ym = fiscal_year_end[:7]
        match = df[df["period_ending"].str.startswith(ym)]
    return match.iloc[0] if not match.empty else None


# ===================================================================
# AAPL — FY2024 (ended Sep 28, 2024)
# Source: Apple 10-K, SEC filing
# ===================================================================


@pytest.mark.validation
class TestAAPLDataAccuracy:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.income = _fetcher.fetch_income_statement("AAPL", period="annual")
        self.row = _get_annual_row(self.income, "2024-09-30")

    def test_income_data_available(self):
        assert self.row is not None, "AAPL FY2024 income data not found"

    def test_revenue(self):
        """AAPL FY2024 revenue: $391,035M per 10-K."""
        assert self.row is not None
        assert self.row["total_revenue"] == pytest.approx(391_035e6, rel=0.01)

    def test_net_income(self):
        """AAPL FY2024 net income: $93,736M per 10-K."""
        assert self.row is not None
        assert self.row["net_income"] == pytest.approx(93_736e6, rel=0.01)

    def test_diluted_eps(self):
        """AAPL FY2024 diluted EPS: $6.08 per 10-K."""
        assert self.row is not None
        assert self.row["diluted_earnings_per_share"] == pytest.approx(6.08, abs=0.05)


# ===================================================================
# GOOGL — FY2024 (ended Dec 31, 2024)
# Source: Alphabet 10-K, SEC filing
# ===================================================================


@pytest.mark.validation
class TestGOOGLDataAccuracy:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.income = _fetcher.fetch_income_statement("GOOGL", period="annual")
        self.row = _get_annual_row(self.income, "2024-12-31")

    def test_income_data_available(self):
        assert self.row is not None, "GOOGL FY2024 income data not found"

    def test_revenue(self):
        """GOOGL FY2024 revenue: $350,018M per 10-K."""
        assert self.row is not None
        assert self.row["total_revenue"] == pytest.approx(350_018e6, rel=0.01)

    def test_net_income(self):
        """GOOGL FY2024 net income: $100,118M per 10-K."""
        assert self.row is not None
        assert self.row["net_income"] == pytest.approx(100_118e6, rel=0.01)

    def test_diluted_eps(self):
        """GOOGL FY2024 diluted EPS: $8.04 per 10-K."""
        assert self.row is not None
        assert self.row["diluted_earnings_per_share"] == pytest.approx(8.04, abs=0.05)


# ===================================================================
# BABA — FY2025 (ended Mar 31, 2025)
# Source: Alibaba press release May 2025
# ===================================================================


@pytest.mark.validation
class TestBABADataAccuracy:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.income = _fetcher.fetch_income_statement("BABA", period="annual")
        self.row = _get_annual_row(self.income, "2025-03-31")

    def test_income_data_available(self):
        assert self.row is not None, "BABA FY2025 income data not found"

    def test_revenue_in_rmb(self):
        """BABA FY2025 revenue: RMB 996,347M — must be in RMB, not USD."""
        assert self.row is not None
        rev = self.row["total_revenue"]
        # Should be ~996B RMB, NOT ~137B USD
        assert rev > 500e9, f"Revenue {rev:.0f} looks like USD, expected RMB ~996B"
        assert rev == pytest.approx(996_347e6, rel=0.01)

    def test_reporting_currency_is_cny(self):
        """BABA reports in CNY, not USD."""
        import yfinance as yf

        info = yf.Ticker("BABA").info
        assert info.get("financialCurrency") == "CNY"


# ===================================================================
# Schema validation — column names and structure
# ===================================================================


@pytest.mark.validation
class TestDataSchema:
    """Verify OpenBB returns expected columns across statement types."""

    def test_income_statement_columns(self):
        df = _fetcher.fetch_income_statement("AAPL", period="annual")
        required = {
            "period_ending",
            "total_revenue",
            "gross_profit",
            "operating_income",
            "net_income",
        }
        assert required.issubset(set(df.columns)), f"Missing columns: {required - set(df.columns)}"

    def test_balance_sheet_columns(self):
        df = _fetcher.fetch_balance_sheet("AAPL", period="annual")
        required = {
            "period_ending",
            "total_assets",
            "total_debt",
            "cash_and_cash_equivalents",
        }
        assert required.issubset(set(df.columns)), f"Missing columns: {required - set(df.columns)}"

    def test_cash_flow_columns(self):
        df = _fetcher.fetch_cash_flow("AAPL", period="annual")
        required = {
            "period_ending",
            "operating_cash_flow",
            "free_cash_flow",
        }
        assert required.issubset(set(df.columns)), f"Missing columns: {required - set(df.columns)}"

    def test_quarterly_data_has_multiple_periods(self):
        df = _fetcher.fetch_income_statement("AAPL", period="quarter")
        assert len(df) >= 4, f"Expected >=4 quarters, got {len(df)}"

    def test_period_ending_is_valid_date(self):
        df = _fetcher.fetch_income_statement("AAPL", period="annual")
        dates = pd.to_datetime(df["period_ending"], errors="coerce")
        assert dates.notna().all(), "Some period_ending values are not valid dates"
