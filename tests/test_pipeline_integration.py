"""Integration tests for pipeline orchestration."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def _make_result(df):
    result = MagicMock()
    result.to_dataframe.return_value = df
    return result


@pytest.fixture
def mock_all_obb(sample_price_df, sample_fundamentals_df, sample_filings_df, sample_economic_df):
    """Mock all OpenBB calls used by the pipeline."""
    patches = [
        patch("watchlist_fetcher.obb"),
        patch("sec_parser.obb"),
        patch("economic_dashboard.obb"),
    ]
    mocks = [p.start() for p in patches]
    for mock in mocks:
        mock.user.preferences.output_type = "dataframe"
        # Use side_effect to return fresh copies to avoid UNIQUE constraint issues
        mock.equity.price.historical.side_effect = lambda **kwargs: _make_result(
            sample_price_df.copy()
        )
        mock.equity.fundamental.metrics.return_value = _make_result(sample_fundamentals_df)
        mock.equity.fundamental.filings.return_value = _make_result(sample_filings_df)
        mock.economy.gdp.real.return_value = _make_result(sample_economic_df)
        mock.economy.gdp.nominal.return_value = _make_result(sample_economic_df)
        mock.economy.cpi.return_value = _make_result(sample_economic_df)
        mock.economy.unemployment.return_value = _make_result(sample_economic_df)
        mock.economy.interest_rates.return_value = _make_result(sample_economic_df)
        mock.economy.fred_series.return_value = _make_result(sample_economic_df)
    yield mocks
    for p in patches:
        p.stop()


def _make_fred_obb_mocks(mocks, sample_price_df, sample_fundamentals_df, sample_filings_df):
    """Configure OBB mocks where FRED series return symbol-named columns."""
    fred_series = {
        "VIXCLS": [18.5, 19.0, 17.8],
        "DGS10": [4.25, 4.30, 4.35],
        "T10Y2Y": [-0.15, -0.10, -0.05],
        "FEDFUNDS": [5.50, 5.50, 5.25],
        "UNRATE": [3.7, 3.8, 3.9],
        "CPIAUCSL": [305.0, 306.0, 307.0],
        "GDP": [27000.0, 27200.0, 27500.0],
    }
    dates = pd.date_range("2024-01-01", periods=3, freq="MS")
    non_fred_df = pd.DataFrame({"date": dates, "value": [1.0, 2.0, 3.0]})

    for mock in mocks:
        mock.user.preferences.output_type = "dataframe"
        mock.equity.price.historical.side_effect = lambda **kwargs: _make_result(
            sample_price_df.copy()
        )
        mock.equity.fundamental.metrics.return_value = _make_result(sample_fundamentals_df)
        mock.equity.fundamental.filings.return_value = _make_result(sample_filings_df)
        mock.economy.gdp.real.return_value = _make_result(non_fred_df.copy())
        mock.economy.gdp.nominal.return_value = _make_result(non_fred_df.copy())
        mock.economy.cpi.return_value = _make_result(non_fred_df.copy())
        mock.economy.unemployment.return_value = _make_result(non_fred_df.copy())
        mock.economy.interest_rates.return_value = _make_result(non_fred_df.copy())

        # FRED series return data with symbol-named columns (the bug scenario)
        def _fred_side_effect(fred_series=fred_series, dates=dates, **kwargs):
            symbol = kwargs.get("symbol", "GDP")
            if symbol in fred_series:
                df = pd.DataFrame({"date": dates, symbol: fred_series[symbol]})
            else:
                df = pd.DataFrame({"date": dates, "value": [0.0, 0.0, 0.0]})
            return _make_result(df)

        mock.economy.fred_series.side_effect = _fred_side_effect


@pytest.mark.integration
class TestRunFullPipeline:
    def test_full_pipeline_completes(self, tmp_db_path, mock_all_obb):
        """Test that the full pipeline runs without errors."""
        with (
            patch("database.DB_PATH", tmp_db_path),
            patch("config.DB_PATH", tmp_db_path),
            patch("config.DATA_DIR", tmp_db_path.parent),
            patch("config.CACHE_DIR", tmp_db_path.parent / "cache"),
        ):
            (tmp_db_path.parent / "cache").mkdir(exist_ok=True)
            from run_pipeline import run_full_pipeline

            # Should complete without raising
            run_full_pipeline()

    def test_pipeline_handles_api_failure(self, tmp_db_path):
        """Test that pipeline handles API failures gracefully."""
        with (
            patch("watchlist_fetcher.obb") as mock_wf_obb,
            patch("sec_parser.obb") as mock_sp_obb,
            patch("economic_dashboard.obb") as mock_ed_obb,
            patch("database.DB_PATH", tmp_db_path),
            patch("config.DB_PATH", tmp_db_path),
            patch("config.DATA_DIR", tmp_db_path.parent),
            patch("config.CACHE_DIR", tmp_db_path.parent / "cache"),
        ):
            (tmp_db_path.parent / "cache").mkdir(exist_ok=True)

            # All API calls fail
            for mock in [mock_wf_obb, mock_sp_obb, mock_ed_obb]:
                mock.user.preferences.output_type = "dataframe"
                mock.equity.price.historical.side_effect = Exception("Network error")
                mock.equity.fundamental.metrics.side_effect = Exception("Network error")
                mock.equity.fundamental.filings.side_effect = Exception("Network error")
                mock.economy.gdp.real.side_effect = Exception("Network error")
                mock.economy.gdp.nominal.side_effect = Exception("Network error")
                mock.economy.cpi.side_effect = Exception("Network error")
                mock.economy.unemployment.side_effect = Exception("Network error")
                mock.economy.interest_rates.side_effect = Exception("Network error")
                mock.economy.fred_series.side_effect = Exception("Network error")

            from run_pipeline import run_full_pipeline

            # Should not raise even with all API failures
            run_full_pipeline()


@pytest.mark.integration
class TestPipelineDbState:
    """Verify that the pipeline actually populates the DB with correct values."""

    def test_pipeline_saves_fred_data_with_values(
        self, tmp_db_path, sample_price_df, sample_fundamentals_df, sample_filings_df
    ):
        """Regression: FRED data with symbol-named columns must be saved
        with non-NULL values after the pipeline runs."""
        patches = [
            patch("watchlist_fetcher.obb"),
            patch("sec_parser.obb"),
            patch("economic_dashboard.obb"),
        ]
        mocks = [p.start() for p in patches]
        _make_fred_obb_mocks(mocks, sample_price_df, sample_fundamentals_df, sample_filings_df)

        try:
            with (
                patch("database.DB_PATH", tmp_db_path),
                patch("config.DB_PATH", tmp_db_path),
                patch("config.DATA_DIR", tmp_db_path.parent),
                patch("config.CACHE_DIR", tmp_db_path.parent / "cache"),
            ):
                (tmp_db_path.parent / "cache").mkdir(exist_ok=True)
                from run_pipeline import run_full_pipeline

                run_full_pipeline()

                # Verify FRED series were saved with non-NULL values
                from database import Database

                db = Database(db_path=tmp_db_path)
                indicators = db.get_latest_economic_indicators(
                    ["VIXCLS", "DGS10", "T10Y2Y", "FEDFUNDS"]
                )
                assert len(indicators) == 4, (
                    f"Expected 4 FRED indicators, got {len(indicators)}: "
                    f"{list(indicators['series_id']) if not indicators.empty else '(empty)'}"
                )
                for _, row in indicators.iterrows():
                    assert pd.notna(row["value"]), f"Series {row['series_id']} has NULL value"

                # Spot-check a specific value (VIXCLS latest = 17.8)
                vix = indicators[indicators["series_id"] == "VIXCLS"]
                assert vix["value"].iloc[0] == 17.8

                # Non-FRED indicators should also be saved
                non_fred = db.get_latest_economic_indicators(["GDP_REAL", "CPI"])
                assert len(non_fred) == 2
                for _, row in non_fred.iterrows():
                    assert pd.notna(row["value"])
        finally:
            for p in patches:
                p.stop()


@pytest.mark.integration
class TestRunQuickTest:
    def test_quick_test_completes(self, tmp_db_path, mock_all_obb):
        with (
            patch("database.DB_PATH", tmp_db_path),
            patch("config.DB_PATH", tmp_db_path),
            patch("config.DATA_DIR", tmp_db_path.parent),
            patch("config.CACHE_DIR", tmp_db_path.parent / "cache"),
        ):
            (tmp_db_path.parent / "cache").mkdir(exist_ok=True)
            from run_pipeline import run_quick_test

            run_quick_test()
