"""Tests for src/economic_dashboard.py."""

import sqlite3
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


class TestEconomicDashboard:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_db_path):
        """Set up EconomicDashboard with mocked obb and temp DB."""
        with patch("economic_dashboard.obb") as self.mock_obb, \
             patch("database.DB_PATH", tmp_db_path), \
             patch("config.DB_PATH", tmp_db_path), \
             patch("config.DATA_DIR", tmp_db_path.parent), \
             patch("config.CACHE_DIR", tmp_db_path.parent / "cache"):
            (tmp_db_path.parent / "cache").mkdir(exist_ok=True)
            from economic_dashboard import EconomicDashboard
            self.dashboard = EconomicDashboard()
            yield

    def _make_result(self, df):
        result = MagicMock()
        result.to_dataframe.return_value = df
        return result

    def test_fetch_fred_series_success(self, sample_economic_df):
        self.mock_obb.economy.fred_series.return_value = self._make_result(sample_economic_df)
        result = self.dashboard.fetch_fred_series("GDP")
        assert result is not None
        assert not result.empty
        assert "series_id" in result.columns
        assert result["series_id"].iloc[0] == "GDP"

    def test_fetch_fred_series_with_dates(self, sample_economic_df):
        self.mock_obb.economy.fred_series.return_value = self._make_result(sample_economic_df)
        self.dashboard.fetch_fred_series("GDP", start_date="2024-01-01", end_date="2024-12-31")
        call_kwargs = self.mock_obb.economy.fred_series.call_args.kwargs
        assert call_kwargs["start_date"] == "2024-01-01"

    def test_fetch_fred_series_error(self):
        self.mock_obb.economy.fred_series.side_effect = Exception("API error")
        result = self.dashboard.fetch_fred_series("GDP")
        assert result is None

    def test_fetch_gdp_real_success(self):
        df = pd.DataFrame({"value": [20000.0], "date": ["2024-06-01"]})
        self.mock_obb.economy.gdp.real.return_value = self._make_result(df)
        result = self.dashboard.fetch_gdp_real()
        assert result is not None
        assert not result.empty

    def test_fetch_gdp_real_error(self):
        self.mock_obb.economy.gdp.real.side_effect = Exception("API error")
        result = self.dashboard.fetch_gdp_real()
        assert result is None

    def test_fetch_gdp_nominal_success(self):
        df = pd.DataFrame({"value": [25000.0]})
        self.mock_obb.economy.gdp.nominal.return_value = self._make_result(df)
        result = self.dashboard.fetch_gdp_nominal()
        assert result is not None

    def test_fetch_cpi_success(self):
        df = pd.DataFrame({"value": [305.0]})
        self.mock_obb.economy.cpi.return_value = self._make_result(df)
        result = self.dashboard.fetch_cpi()
        assert result is not None

    def test_fetch_unemployment_success(self):
        df = pd.DataFrame({"value": [3.7]})
        self.mock_obb.economy.unemployment.return_value = self._make_result(df)
        result = self.dashboard.fetch_unemployment()
        assert result is not None

    def test_fetch_interest_rates_success(self):
        df = pd.DataFrame({"value": [5.25]})
        self.mock_obb.economy.interest_rates.return_value = self._make_result(df)
        result = self.dashboard.fetch_interest_rates()
        assert result is not None

    def test_update_all_indicators(self):
        indicator_df = pd.DataFrame({"date": ["2024-06-01"], "value": [1.0]})
        empty_result = self._make_result(indicator_df)
        self.mock_obb.economy.gdp.real.return_value = empty_result
        self.mock_obb.economy.gdp.nominal.return_value = empty_result
        self.mock_obb.economy.cpi.return_value = empty_result
        self.mock_obb.economy.unemployment.return_value = empty_result
        self.mock_obb.economy.interest_rates.return_value = empty_result
        self.mock_obb.economy.fred_series.return_value = empty_result

        # Should not raise
        self.dashboard.update_all_indicators()

    def test_get_economic_summary_empty(self):
        result = self.dashboard.get_economic_summary()
        assert isinstance(result, pd.DataFrame)

    def test_get_economic_summary_with_data(self, sample_economic_df):
        # Save some data first
        self.dashboard.db.save_economic_indicators(sample_economic_df, "GDP")
        result = self.dashboard.get_economic_summary()
        assert not result.empty
        assert "series_id" in result.columns

    def test_generate_dashboard_report_empty(self):
        # All fetches return None
        self.mock_obb.economy.gdp.real.side_effect = Exception("no data")
        self.mock_obb.economy.cpi.side_effect = Exception("no data")
        self.mock_obb.economy.unemployment.side_effect = Exception("no data")
        report = self.dashboard.generate_dashboard_report()
        assert "Economic Indicators Dashboard" in report

    def test_generate_dashboard_report_with_data(self):
        gdp_df = pd.DataFrame({"value": [20000.0]})
        self.mock_obb.economy.gdp.real.return_value = self._make_result(gdp_df)
        self.mock_obb.economy.cpi.side_effect = Exception("no data")
        self.mock_obb.economy.unemployment.side_effect = Exception("no data")
        report = self.dashboard.generate_dashboard_report()
        assert "GDP" in report
