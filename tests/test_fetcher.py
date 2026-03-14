"""Tests for src/fetcher.py."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestDataFetcher:
    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up DataFetcher with mocked obb."""
        with patch("fetcher.obb") as self.mock_obb:
            from fetcher import DataFetcher

            self.fetcher = DataFetcher()
            yield

    def _make_result(self, df):
        """Create a mock OBB result that returns a DataFrame."""
        result = MagicMock()
        result.to_dataframe.return_value = df
        return result

    def test_fetch_historical_prices_success(self, sample_price_df):
        self.mock_obb.equity.price.historical.return_value = self._make_result(sample_price_df)
        result = self.fetcher.fetch_historical_prices("AAPL")
        assert not result.empty
        assert len(result) == 5
        self.mock_obb.equity.price.historical.assert_called_once()

    def test_fetch_historical_prices_with_dates(self, sample_price_df):
        self.mock_obb.equity.price.historical.return_value = self._make_result(sample_price_df)
        self.fetcher.fetch_historical_prices("AAPL", start_date="2025-01-01", end_date="2025-01-31")
        call_kwargs = self.mock_obb.equity.price.historical.call_args
        assert call_kwargs.kwargs["start_date"] == "2025-01-01"
        assert call_kwargs.kwargs["end_date"] == "2025-01-31"

    def test_fetch_historical_prices_error(self):
        self.mock_obb.equity.price.historical.side_effect = Exception("API error")
        result = self.fetcher.fetch_historical_prices("AAPL")
        assert result.empty

    def test_fetch_income_statement_success(self):
        df = pd.DataFrame({"revenue": [100000], "net_income": [20000]})
        self.mock_obb.equity.fundamental.income.return_value = self._make_result(df)
        result = self.fetcher.fetch_income_statement("AAPL")
        assert not result.empty

    def test_fetch_income_statement_error(self):
        self.mock_obb.equity.fundamental.income.side_effect = Exception("API error")
        result = self.fetcher.fetch_income_statement("AAPL")
        assert result.empty

    def test_fetch_balance_sheet_success(self):
        df = pd.DataFrame({"total_assets": [300000]})
        self.mock_obb.equity.fundamental.balance.return_value = self._make_result(df)
        result = self.fetcher.fetch_balance_sheet("AAPL")
        assert not result.empty

    def test_fetch_cash_flow_success(self):
        df = pd.DataFrame({"operating_cash_flow": [50000]})
        self.mock_obb.equity.fundamental.cash.return_value = self._make_result(df)
        result = self.fetcher.fetch_cash_flow("AAPL")
        assert not result.empty

    def test_fetch_metrics_success(self, sample_fundamentals_df):
        self.mock_obb.equity.fundamental.metrics.return_value = self._make_result(
            sample_fundamentals_df
        )
        result = self.fetcher.fetch_metrics("AAPL")
        assert not result.empty

    def test_fetch_sec_filings_success(self, sample_filings_df):
        self.mock_obb.equity.fundamental.filings.return_value = self._make_result(sample_filings_df)
        result = self.fetcher.fetch_sec_filings("AAPL", limit=2)
        assert len(result) == 2

    def test_fetch_sec_filings_error(self):
        self.mock_obb.equity.fundamental.filings.side_effect = Exception("API error")
        result = self.fetcher.fetch_sec_filings("AAPL")
        assert result.empty

    def test_fetch_profile_success(self):
        mock_result = MagicMock()
        mock_result.results = [{"name": "Apple Inc", "sector": "Technology"}]
        self.mock_obb.equity.profile.return_value = mock_result
        result = self.fetcher.fetch_profile("AAPL")
        assert result["name"] == "Apple Inc"

    def test_fetch_profile_error(self):
        self.mock_obb.equity.profile.side_effect = Exception("API error")
        result = self.fetcher.fetch_profile("AAPL")
        assert result == {}

    def test_fetch_profile_no_results(self):
        mock_result = MagicMock()
        mock_result.results = []
        self.mock_obb.equity.profile.return_value = mock_result
        result = self.fetcher.fetch_profile("AAPL")
        assert result == {}
