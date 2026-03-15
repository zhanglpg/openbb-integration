"""Tests for None-value handling in dashboard indicator formatting.

Regression tests for:
  - TypeError: unsupported format string passed to NoneType.__format__
  - AttributeError: 'Series' object has no attribute 'columns'
"""

import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def _make_econ_df(values):
    """Build an economic indicators DataFrame with given values dict.

    Args:
        values: dict mapping series_id to value (or None).
    """
    rows = []
    for series_id, value in values.items():
        rows.append({"series_id": series_id, "date": "2025-03-01", "value": value})
    return pd.DataFrame(rows)


class TestDashboardNoneIndicatorValues:
    """Regression: dashboard.py must not crash when indicator values are None."""

    @pytest.fixture(autouse=True)
    def setup_mocks(self):
        """Mock streamlit and other imports so dashboard.py can be imported."""
        self.mock_st = MagicMock()
        # st.columns() returns a list of mock columns
        self.mock_st.columns.return_value = [MagicMock() for _ in range(4)]
        self.mock_st.sidebar.selectbox.return_value = "AAPL"
        self.mock_st.cache_data = MagicMock(return_value=lambda f: f)
        self.mock_st.cache_resource = MagicMock(return_value=lambda f: f)

        with patch.dict(sys.modules, {"streamlit": self.mock_st}):
            yield

    def test_format_none_value_raises_without_fix(self):
        """Demonstrate that formatting None with :.2f raises TypeError."""
        with pytest.raises(TypeError, match="unsupported format string"):
            value = None
            f"{value:.2f}"

    def test_format_valid_value_succeeds(self):
        """Formatting a float with :.2f works as expected."""
        value = 4.25
        assert f"{value:.2f}" == "4.25"

    def test_dashboard_none_value_displays_na(self):
        """dashboard.py: None indicator value should display 'N/A', not crash."""
        econ_df = _make_econ_df(
            {
                "VIXCLS": None,
                "DGS10": 4.25,
                "T10Y2Y": None,
                "FEDFUNDS": 5.50,
            }
        )

        mock_st = self.mock_st

        # Simulate the dashboard.py formatting loop (lines 207-226)
        friendly_names = {
            "VIXCLS": "VIX (Volatility)",
            "DGS10": "10Y Treasury",
            "T10Y2Y": "Yield Curve (10Y-2Y)",
            "FEDFUNDS": "Fed Funds Rate",
        }
        key_indicators = ["VIXCLS", "DGS10", "T10Y2Y", "FEDFUNDS"]

        calls = {}
        for series_id in key_indicators:
            row = econ_df[econ_df["series_id"] == series_id]
            if not row.empty:
                value = row["value"].iloc[-1]
                date = row["date"].iloc[-1]
                name = friendly_names.get(series_id, series_id)

                if pd.isna(value):
                    mock_st.metric(name, "N/A", delta=f"As of {date}")
                    calls[series_id] = "N/A"
                    continue

                # Format value
                if series_id in ["VIXCLS", "DGS10", "T10Y2Y", "FEDFUNDS"]:
                    formatted_value = f"{value:.2f}"
                    if series_id == "DGS10" or series_id == "FEDFUNDS":
                        formatted_value += "%"
                else:
                    formatted_value = f"{value:.3f}"

                mock_st.metric(name, formatted_value, delta=f"As of {date}")
                calls[series_id] = formatted_value

        # None values should show "N/A"
        assert calls["VIXCLS"] == "N/A"
        assert calls["T10Y2Y"] == "N/A"
        # Valid values should be formatted correctly
        assert calls["DGS10"] == "4.25%"
        assert calls["FEDFUNDS"] == "5.50%"

    def test_dashboard_all_none_values(self):
        """dashboard.py: all None values should display 'N/A' without crash."""
        econ_df = _make_econ_df(
            {
                "VIXCLS": None,
                "DGS10": None,
                "T10Y2Y": None,
                "FEDFUNDS": None,
            }
        )

        for series_id in ["VIXCLS", "DGS10", "T10Y2Y", "FEDFUNDS"]:
            row = econ_df[econ_df["series_id"] == series_id]
            if not row.empty:
                value = row["value"].iloc[-1]
                if pd.isna(value):
                    continue  # This is the fix — without it, next line would crash
                f"{value:.2f}"  # Would raise TypeError if value is None

    def test_dashboard_all_valid_values(self):
        """dashboard.py: all valid values should format correctly."""
        econ_df = _make_econ_df(
            {
                "VIXCLS": 18.5,
                "DGS10": 4.25,
                "T10Y2Y": -0.15,
                "FEDFUNDS": 5.50,
            }
        )

        key_indicators = ["VIXCLS", "DGS10", "T10Y2Y", "FEDFUNDS"]
        results = {}

        for series_id in key_indicators:
            row = econ_df[econ_df["series_id"] == series_id]
            if not row.empty:
                value = row["value"].iloc[-1]
                if pd.isna(value):
                    results[series_id] = "N/A"
                    continue
                formatted = f"{value:.2f}"
                if series_id in ("DGS10", "FEDFUNDS"):
                    formatted += "%"
                results[series_id] = formatted

        assert results == {
            "VIXCLS": "18.50",
            "DGS10": "4.25%",
            "T10Y2Y": "-0.15",
            "FEDFUNDS": "5.50%",
        }


class TestGetCol:
    """Regression: _get_col must work on both DataFrames and Series (iloc rows).

    Bug: _get_col used `name in df.columns`, but `df.iloc[-1]` returns a Series
    which has no `.columns` attribute, causing AttributeError.
    """

    def _get_col(self, obj, name, default=None):
        """Mirror the fixed _get_col from pages/4_Research.py."""
        try:
            if name in obj.index if isinstance(obj, pd.Series) else name in obj.columns:
                return obj[name]
        except (AttributeError, KeyError):
            pass
        return default

    def test_get_col_from_dataframe(self):
        """_get_col works on a DataFrame (get column)."""
        df = pd.DataFrame({"total_revenue": [100, 200], "net_income": [10, 20]})
        assert self._get_col(df, "total_revenue").tolist() == [100, 200]

    def test_get_col_from_series(self):
        """_get_col works on a Series (a single row from iloc)."""
        df = pd.DataFrame({"total_revenue": [100, 200], "net_income": [10, 20]})
        row = df.iloc[-1]  # This is a Series
        assert self._get_col(row, "total_revenue") == 200

    def test_get_col_missing_from_dataframe(self):
        """Missing column in DataFrame returns default."""
        df = pd.DataFrame({"total_revenue": [100]})
        assert self._get_col(df, "nonexistent") is None
        assert self._get_col(df, "nonexistent", "N/A") == "N/A"

    def test_get_col_missing_from_series(self):
        """Missing key in Series returns default."""
        row = pd.Series({"total_revenue": 100})
        assert self._get_col(row, "nonexistent") is None
        assert self._get_col(row, "nonexistent", 0) == 0

    def test_get_col_none_input(self):
        """None input returns default without crash."""
        assert self._get_col(None, "anything") is None

    def test_get_col_series_with_nan(self):
        """Series with NaN value returns the NaN (not the default)."""
        row = pd.Series({"total_revenue": float("nan")})
        result = self._get_col(row, "total_revenue")
        assert pd.isna(result)


class TestEconomyPageNoneIndicatorValues:
    """Regression: pages/2_Economy.py must not crash when indicator values are None."""

    def test_economy_page_none_value_displays_na(self):
        """2_Economy.py: None indicator value should display 'N/A', not crash."""
        econ_df = _make_econ_df(
            {
                "VIXCLS": None,
                "DGS10": 4.25,
                "T10Y2Y": None,
                "FEDFUNDS": 5.50,
            }
        )

        friendly_names = {
            "VIXCLS": "VIX (Volatility)",
            "DGS10": "10Y Treasury",
            "T10Y2Y": "Yield Curve (10Y-2Y)",
            "FEDFUNDS": "Fed Funds Rate",
        }

        cols = [MagicMock() for _ in range(4)]
        calls = {}

        # Simulate the 2_Economy.py formatting loop (lines 130-142)
        for i, series_id in enumerate(["VIXCLS", "DGS10", "T10Y2Y", "FEDFUNDS"]):
            row = econ_df[econ_df["series_id"] == series_id]
            if not row.empty:
                value = row["value"].iloc[-1]
                date = row["date"].iloc[-1]
                name = friendly_names.get(series_id, series_id)
                if pd.isna(value):
                    cols[i].metric(name, "N/A", delta=f"As of {date}")
                    calls[series_id] = "N/A"
                    continue
                formatted = f"{value:.2f}"
                if series_id in ("DGS10", "FEDFUNDS"):
                    formatted += "%"
                cols[i].metric(name, formatted, delta=f"As of {date}")
                calls[series_id] = formatted

        assert calls["VIXCLS"] == "N/A"
        assert calls["T10Y2Y"] == "N/A"
        assert calls["DGS10"] == "4.25%"
        assert calls["FEDFUNDS"] == "5.50%"

    def test_economy_page_all_none_does_not_crash(self):
        """2_Economy.py: all None values should not crash."""
        econ_df = _make_econ_df(
            {
                "VIXCLS": None,
                "DGS10": None,
                "T10Y2Y": None,
                "FEDFUNDS": None,
            }
        )

        for i, series_id in enumerate(["VIXCLS", "DGS10", "T10Y2Y", "FEDFUNDS"]):
            row = econ_df[econ_df["series_id"] == series_id]
            if not row.empty:
                value = row["value"].iloc[-1]
                if pd.isna(value):
                    continue  # The fix
                f"{value:.2f}"  # Would raise TypeError without the fix


# ===================================================================
# _fmt_large currency support
# ===================================================================


class TestFmtLargeCurrency:
    """Regression: _fmt_large must display correct currency symbol.

    Bug: BABA reports financials in CNY but _fmt_large always showed $.
    Foreign-listed companies (BABA=CNY, TSM=TWD, TCEHY=CNY) had misleading
    dollar signs on their revenue, EBITDA, and cash flow figures.
    """

    def _fmt_large(self, value, fallback="N/A", currency="$"):
        """Mirror _fmt_large from pages/4_Research.py."""
        if value is None:
            return fallback
        try:
            if pd.isna(value):
                return fallback
        except (TypeError, ValueError):
            pass
        sign = "-" if value < 0 else ""
        abs_val = abs(value)
        if abs_val >= 1e12:
            return f"{sign}{currency}{abs_val / 1e12:.2f}T"
        if abs_val >= 1e9:
            return f"{sign}{currency}{abs_val / 1e9:.2f}B"
        if abs_val >= 1e6:
            return f"{sign}{currency}{abs_val / 1e6:.1f}M"
        return f"{sign}{currency}{abs_val:,.0f}"

    def test_default_usd(self):
        assert self._fmt_large(100e9) == "$100.00B"

    def test_cny_currency(self):
        """BABA revenue ~996B CNY should show \u00a5 symbol."""
        assert self._fmt_large(996_347_000_000, currency="\u00a5") == "\u00a5996.35B"

    def test_twd_currency(self):
        """TSM revenue in TWD should show NT$ prefix."""
        assert self._fmt_large(2_200_000_000_000, currency="NT$") == "NT$2.20T"

    def test_negative_value_with_currency(self):
        """Negative values should show sign before currency symbol."""
        assert self._fmt_large(-50e9, currency="\u00a5") == "-\u00a550.00B"

    def test_none_with_currency(self):
        assert self._fmt_large(None, currency="\u00a5") == "N/A"

    def test_nan_with_currency(self):
        assert self._fmt_large(float("nan"), currency="\u00a5") == "N/A"

    def test_millions(self):
        assert self._fmt_large(500e6, currency="\u20ac") == "\u20ac500.0M"

    def test_small_value(self):
        assert self._fmt_large(12345, currency="\u00a3") == "\u00a312,345"


class TestComputeTtmIncomeWithSBC:
    """Regression: TTM must include stock_based_compensation for adjusted EBITDA.

    Bug: TTM computation didn't sum SBC from cash flow, causing adjusted EBITDA
    to be incorrect in trailing 4Q view.
    """

    def test_sbc_summed_in_ttm(self):
        """SBC should be summed across 4 quarters in TTM."""
        import sys
        from pathlib import Path

        sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
        from analysis import compute_ttm

        df = pd.DataFrame(
            {
                "period_ending": pd.date_range("2024-03-31", periods=4, freq="QE").strftime(
                    "%Y-%m-%d"
                ),
                "stock_based_compensation": [2e9, 2.5e9, 3e9, 3.5e9],
                "operating_cash_flow": [25e9, 30e9, 28e9, 35e9],
            }
        )
        result = compute_ttm(df, sum_cols=["stock_based_compensation", "operating_cash_flow"])
        assert len(result) == 1
        assert result["stock_based_compensation"].iloc[0] == pytest.approx(11e9)
        assert result["operating_cash_flow"].iloc[0] == pytest.approx(118e9)
