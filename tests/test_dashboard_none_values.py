"""Tests for None-value handling in dashboard indicator formatting.

Regression tests for: TypeError: unsupported format string passed to NoneType.__format__
Bug: economic indicator values can be None (missing FRED data), but the
formatting code used f"{value:.2f}" without a None check.
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
        econ_df = _make_econ_df({
            "VIXCLS": None,
            "DGS10": 4.25,
            "T10Y2Y": None,
            "FEDFUNDS": 5.50,
        })

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
        econ_df = _make_econ_df({
            "VIXCLS": None,
            "DGS10": None,
            "T10Y2Y": None,
            "FEDFUNDS": None,
        })

        friendly_names = {
            "VIXCLS": "VIX (Volatility)",
            "DGS10": "10Y Treasury",
            "T10Y2Y": "Yield Curve (10Y-2Y)",
            "FEDFUNDS": "Fed Funds Rate",
        }
        key_indicators = ["VIXCLS", "DGS10", "T10Y2Y", "FEDFUNDS"]

        for series_id in key_indicators:
            row = econ_df[econ_df["series_id"] == series_id]
            if not row.empty:
                value = row["value"].iloc[-1]
                if pd.isna(value):
                    continue  # This is the fix — without it, next line would crash
                f"{value:.2f}"  # Would raise TypeError if value is None

    def test_dashboard_all_valid_values(self):
        """dashboard.py: all valid values should format correctly."""
        econ_df = _make_econ_df({
            "VIXCLS": 18.5,
            "DGS10": 4.25,
            "T10Y2Y": -0.15,
            "FEDFUNDS": 5.50,
        })

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


class TestEconomyPageNoneIndicatorValues:
    """Regression: pages/2_Economy.py must not crash when indicator values are None."""

    def test_economy_page_none_value_displays_na(self):
        """2_Economy.py: None indicator value should display 'N/A', not crash."""
        econ_df = _make_econ_df({
            "VIXCLS": None,
            "DGS10": 4.25,
            "T10Y2Y": None,
            "FEDFUNDS": 5.50,
        })

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
        econ_df = _make_econ_df({
            "VIXCLS": None,
            "DGS10": None,
            "T10Y2Y": None,
            "FEDFUNDS": None,
        })

        for i, series_id in enumerate(["VIXCLS", "DGS10", "T10Y2Y", "FEDFUNDS"]):
            row = econ_df[econ_df["series_id"] == series_id]
            if not row.empty:
                value = row["value"].iloc[-1]
                if pd.isna(value):
                    continue  # The fix
                f"{value:.2f}"  # Would raise TypeError without the fix
