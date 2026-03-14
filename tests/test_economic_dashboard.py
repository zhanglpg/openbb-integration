"""Tests for src/economic_dashboard.py."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from economic_dashboard import _normalize_dataframe


class TestNormalizeDataframe:
    """Tests for _normalize_dataframe() — column name normalisation."""

    def test_passthrough_when_columns_already_correct(self):
        df = pd.DataFrame({"date": ["2024-01-01"], "value": [42.0]})
        result = _normalize_dataframe(df)
        assert "date" in result.columns
        assert "value" in result.columns
        assert result["value"].iloc[0] == 42.0

    def test_series_symbol_as_value_column(self):
        """FRED returns value column named after the series (e.g. 'VIXCLS')."""
        df = pd.DataFrame({"date": ["2024-01-01"], "VIXCLS": [18.5]})
        result = _normalize_dataframe(df, series_id="VIXCLS")
        assert "value" in result.columns
        assert result["value"].iloc[0] == 18.5

    def test_close_column_renamed_to_value(self):
        df = pd.DataFrame({"date": ["2024-01-01"], "close": [155.0]})
        result = _normalize_dataframe(df)
        assert "value" in result.columns
        assert result["value"].iloc[0] == 155.0

    def test_datetime_index_reset(self):
        dates = pd.to_datetime(["2024-01-01", "2024-02-01"])
        df = pd.DataFrame({"value": [1.0, 2.0]}, index=dates)
        df.index.name = "date"
        result = _normalize_dataframe(df)
        assert "date" in result.columns
        assert "value" in result.columns
        assert len(result) == 2

    def test_fallback_to_first_numeric_column(self):
        df = pd.DataFrame({"date": ["2024-01-01"], "some_metric": [99.9]})
        result = _normalize_dataframe(df)
        assert "value" in result.columns
        assert result["value"].iloc[0] == 99.9

    def test_period_column_renamed_to_date(self):
        df = pd.DataFrame({"period": ["2024-01-01"], "value": [7.5]})
        result = _normalize_dataframe(df)
        assert "date" in result.columns
        assert result["date"].iloc[0] == "2024-01-01"

    def test_series_id_takes_priority_over_close(self):
        """When both the series column and 'close' exist, prefer series_id."""
        df = pd.DataFrame({
            "date": ["2024-01-01"],
            "DGS10": [4.25],
            "close": [100.0],
        })
        result = _normalize_dataframe(df, series_id="DGS10")
        assert result["value"].iloc[0] == 4.25


class TestEconomicDashboard:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_db_path):
        """Set up EconomicDashboard with mocked obb and temp DB."""
        with (
            patch("economic_dashboard.obb") as self.mock_obb,
            patch("database.DB_PATH", tmp_db_path),
            patch("config.DB_PATH", tmp_db_path),
            patch("config.DATA_DIR", tmp_db_path.parent),
            patch("config.CACHE_DIR", tmp_db_path.parent / "cache"),
        ):
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

    def test_fetch_fred_series_symbol_column(self):
        """Regression: FRED returns value in a column named after the series."""
        df = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=3, freq="MS"),
            "VIXCLS": [18.5, 19.0, 17.8],
        })
        self.mock_obb.economy.fred_series.return_value = self._make_result(df)
        result = self.dashboard.fetch_fred_series("VIXCLS")
        assert result is not None
        assert "value" in result.columns
        assert result["value"].iloc[0] == 18.5

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

    def test_get_economic_summary_multiple_series(self, sample_economic_df):
        """Regression: get_economic_summary must return latest value per series.
        Original bug: Path.connect() crash; fix delegates to db.get_latest_economic_indicators()."""
        self.dashboard.db.save_economic_indicators(sample_economic_df, "GDP")
        self.dashboard.db.save_economic_indicators(sample_economic_df, "CPI")
        self.dashboard.db.save_economic_indicators(sample_economic_df, "UNRATE")
        result = self.dashboard.get_economic_summary()
        assert len(result) == 3
        assert set(result["series_id"]) == {"GDP", "CPI", "UNRATE"}
        # Each row should have the latest date from the sample data
        for _, row in result.iterrows():
            assert pd.notna(row["value"])
            assert pd.notna(row["date"])

    def test_fred_symbol_column_full_roundtrip(self):
        """Regression: FRED data with symbol-named column flows through
        fetch → normalize → save → read and returns non-NULL values."""
        # Simulate FRED returning data with the series symbol as column name
        fred_df = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=5, freq="MS"),
            "VIXCLS": [18.5, 19.0, 17.8, 20.1, 16.3],
        })
        self.mock_obb.economy.fred_series.return_value = self._make_result(fred_df)

        # Step 1: Fetch (normalizes columns internally)
        fetched = self.dashboard.fetch_fred_series("VIXCLS")
        assert fetched is not None
        assert "value" in fetched.columns
        assert fetched["value"].notna().all()

        # Step 2: Save to DB
        self.dashboard.db.save_economic_indicators(fetched, "VIXCLS")

        # Step 3: Read back via the same path the dashboard uses
        result = self.dashboard.db.get_latest_economic_indicators(["VIXCLS"])
        assert len(result) == 1
        assert result["series_id"].iloc[0] == "VIXCLS"
        assert pd.notna(result["value"].iloc[0])
        assert result["value"].iloc[0] == 16.3  # latest value

    def test_partial_series_in_db(self):
        """Dashboard handles DB having data for some series but not others."""
        # Save data for only 2 of the 4 key indicators
        df = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=3, freq="MS"),
            "value": [4.25, 4.30, 4.35],
        })
        self.dashboard.db.save_economic_indicators(df, "DGS10")
        self.dashboard.db.save_economic_indicators(df, "FEDFUNDS")

        # Request all 4 key indicators — should return only the 2 that exist
        result = self.dashboard.db.get_latest_economic_indicators(
            ["VIXCLS", "DGS10", "T10Y2Y", "FEDFUNDS"]
        )
        assert len(result) == 2
        assert set(result["series_id"]) == {"DGS10", "FEDFUNDS"}
        # Values should be non-NULL
        for _, row in result.iterrows():
            assert pd.notna(row["value"])
            assert row["value"] == 4.35

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
