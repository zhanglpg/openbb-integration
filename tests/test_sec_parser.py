"""Tests for src/sec_parser.py."""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta


class TestSECParser:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_db_path):
        """Set up SECParser with mocked obb and temp DB."""
        with patch("sec_parser.obb") as self.mock_obb, \
             patch("database.DB_PATH", tmp_db_path), \
             patch("config.DB_PATH", tmp_db_path), \
             patch("config.DATA_DIR", tmp_db_path.parent), \
             patch("config.CACHE_DIR", tmp_db_path.parent / "cache"):
            (tmp_db_path.parent / "cache").mkdir(exist_ok=True)
            from sec_parser import SECParser
            self.parser = SECParser()
            yield

    def _make_result(self, df):
        result = MagicMock()
        result.to_dataframe.return_value = df
        return result

    def test_fetch_filings_success(self, sample_filings_df):
        self.mock_obb.equity.fundamental.filings.return_value = self._make_result(sample_filings_df)
        result = self.parser.fetch_filings("AAPL")
        assert not result.empty
        assert len(result) == 3

    def test_fetch_filings_with_type_filter(self, sample_filings_df):
        self.mock_obb.equity.fundamental.filings.return_value = self._make_result(sample_filings_df)
        result = self.parser.fetch_filings("AAPL", filing_types=["10-K"])
        assert len(result) == 1
        assert result["report_type"].iloc[0] == "10-K"

    def test_fetch_filings_with_limit(self, sample_filings_df):
        self.mock_obb.equity.fundamental.filings.return_value = self._make_result(sample_filings_df)
        result = self.parser.fetch_filings("AAPL", limit=2)
        assert len(result) == 2

    def test_fetch_filings_none_response(self):
        self.mock_obb.equity.fundamental.filings.return_value = None
        result = self.parser.fetch_filings("AAPL")
        assert result.empty

    def test_fetch_filings_error(self):
        self.mock_obb.equity.fundamental.filings.side_effect = Exception("API error")
        result = self.parser.fetch_filings("AAPL")
        assert result.empty

    def test_get_latest_10k(self, sample_filings_df):
        self.mock_obb.equity.fundamental.filings.return_value = self._make_result(sample_filings_df)
        result = self.parser.get_latest_10k("AAPL")
        assert result is not None
        assert result["report_type"] == "10-K"
        assert result["symbol"] == "AAPL"

    def test_get_latest_10k_not_found(self):
        df = pd.DataFrame({
            "filing_date": ["2025-07-15"],
            "report_type": ["8-K"],
            "report_url": ["https://sec.gov/8k"],
            "report_date": ["2025-07-10"],
            "accession_number": ["0001-25-000003"],
            "filing_detail_url": ["https://sec.gov/detail/8k"],
            "primary_doc": ["doc3.htm"],
        })
        self.mock_obb.equity.fundamental.filings.return_value = self._make_result(df)
        result = self.parser.get_latest_10k("AAPL")
        assert result is None

    def test_get_latest_10q(self, sample_filings_df):
        self.mock_obb.equity.fundamental.filings.return_value = self._make_result(sample_filings_df)
        result = self.parser.get_latest_10q("AAPL")
        assert result is not None
        assert result["report_type"] == "10-Q"

    def test_get_recent_8k(self, sample_filings_df):
        # Make recent 8-K filings
        recent_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        df = pd.DataFrame({
            "filing_date": [recent_date],
            "report_type": ["8-K"],
            "report_url": ["https://sec.gov/8k"],
            "report_date": [recent_date],
            "accession_number": ["0001-25-000010"],
            "filing_detail_url": ["https://sec.gov/detail/8k"],
            "primary_doc": ["doc.htm"],
            "primary_doc_description": ["Current Report"],
        })
        self.mock_obb.equity.fundamental.filings.return_value = self._make_result(df)
        result = self.parser.get_recent_8k("AAPL", days=30)
        assert len(result) == 1

    def test_get_recent_8k_excludes_old(self):
        old_date = "2020-01-01"
        df = pd.DataFrame({
            "filing_date": [old_date],
            "report_type": ["8-K"],
            "report_url": ["https://sec.gov/8k"],
            "report_date": [old_date],
            "accession_number": ["0001-20-000001"],
            "filing_detail_url": ["https://sec.gov/detail/8k"],
            "primary_doc": ["doc.htm"],
            "primary_doc_description": ["Old Report"],
        })
        self.mock_obb.equity.fundamental.filings.return_value = self._make_result(df)
        result = self.parser.get_recent_8k("AAPL", days=30)
        assert result.empty

    def test_analyze_filing_frequency(self, sample_filings_df):
        self.mock_obb.equity.fundamental.filings.return_value = self._make_result(sample_filings_df)
        result = self.parser.analyze_filing_frequency("AAPL")
        assert result["symbol"] == "AAPL"
        assert result["total_filings"] == 3
        assert "filing_types" in result

    def test_analyze_filing_frequency_empty(self):
        self.mock_obb.equity.fundamental.filings.return_value = self._make_result(pd.DataFrame())
        result = self.parser.analyze_filing_frequency("AAPL")
        assert "error" in result

    def test_compare_filings(self, sample_filings_df):
        self.mock_obb.equity.fundamental.filings.return_value = self._make_result(sample_filings_df)
        result = self.parser.compare_filings(["AAPL", "MSFT"])
        assert len(result) == 2
        assert list(result["symbol"]) == ["AAPL", "MSFT"]

    def test_generate_filing_report(self, sample_filings_df):
        self.mock_obb.equity.fundamental.filings.return_value = self._make_result(sample_filings_df)
        report = self.parser.generate_filing_report("AAPL")
        assert "AAPL" in report
        assert "10-K" in report
        assert isinstance(report, str)
