"""Integration tests for pipeline orchestration."""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


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
        mock.equity.price.historical.side_effect = lambda **kwargs: _make_result(sample_price_df.copy())
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


@pytest.mark.integration
class TestRunFullPipeline:
    def test_full_pipeline_completes(self, tmp_db_path, mock_all_obb):
        """Test that the full pipeline runs without errors."""
        with patch("database.DB_PATH", tmp_db_path), \
             patch("config.DB_PATH", tmp_db_path), \
             patch("config.DATA_DIR", tmp_db_path.parent), \
             patch("config.CACHE_DIR", tmp_db_path.parent / "cache"):
            (tmp_db_path.parent / "cache").mkdir(exist_ok=True)
            from run_pipeline import run_full_pipeline
            # Should complete without raising
            run_full_pipeline()

    def test_pipeline_handles_api_failure(self, tmp_db_path):
        """Test that pipeline handles API failures gracefully."""
        with patch("watchlist_fetcher.obb") as mock_wf_obb, \
             patch("sec_parser.obb") as mock_sp_obb, \
             patch("economic_dashboard.obb") as mock_ed_obb, \
             patch("database.DB_PATH", tmp_db_path), \
             patch("config.DB_PATH", tmp_db_path), \
             patch("config.DATA_DIR", tmp_db_path.parent), \
             patch("config.CACHE_DIR", tmp_db_path.parent / "cache"):
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
class TestRunQuickTest:
    def test_quick_test_completes(self, tmp_db_path, mock_all_obb):
        with patch("database.DB_PATH", tmp_db_path), \
             patch("config.DB_PATH", tmp_db_path), \
             patch("config.DATA_DIR", tmp_db_path.parent), \
             patch("config.CACHE_DIR", tmp_db_path.parent / "cache"):
            (tmp_db_path.parent / "cache").mkdir(exist_ok=True)
            from run_pipeline import run_quick_test
            run_quick_test()
