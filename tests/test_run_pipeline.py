"""Tests for src/run_pipeline.py — CLI entry point and daily report generation."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


# ===================================================================
# CLI argument parsing — main()
# ===================================================================


@pytest.mark.unit
class TestMainArgparse:
    """Verify main() dispatches to the correct function for each mode."""

    @patch("run_pipeline.run_full_pipeline")
    def test_mode_full(self, mock_fn):
        with patch("sys.argv", ["run_pipeline.py", "full"]):
            import run_pipeline

            run_pipeline.main()
        mock_fn.assert_called_once()

    @patch("run_pipeline.run_quick_test")
    def test_mode_test(self, mock_fn):
        with patch("sys.argv", ["run_pipeline.py", "test"]):
            import run_pipeline

            run_pipeline.main()
        mock_fn.assert_called_once()

    @patch("run_pipeline.run_daily_report")
    def test_mode_report(self, mock_fn):
        with patch("sys.argv", ["run_pipeline.py", "report"]):
            import run_pipeline

            run_pipeline.main()
        mock_fn.assert_called_once()

    @patch("run_pipeline.run_daily_report")
    @patch("run_pipeline.run_full_pipeline")
    def test_mode_daily(self, mock_full, mock_report):
        with patch("sys.argv", ["run_pipeline.py", "daily"]):
            import run_pipeline

            run_pipeline.main()
        mock_full.assert_called_once()
        mock_report.assert_called_once()

    @patch("run_pipeline.WatchlistFetcher")
    def test_mode_prices(self, mock_wf_cls):
        mock_wf = MagicMock()
        mock_wf_cls.return_value = mock_wf
        with patch("sys.argv", ["run_pipeline.py", "prices"]):
            import run_pipeline

            run_pipeline.main()
        mock_wf.update_all_prices.assert_called_once()

    @patch("run_pipeline.WatchlistFetcher")
    def test_mode_fundamentals(self, mock_wf_cls):
        mock_wf = MagicMock()
        mock_wf_cls.return_value = mock_wf
        with patch("sys.argv", ["run_pipeline.py", "fundamentals"]):
            import run_pipeline

            run_pipeline.main()
        mock_wf.update_all_fundamentals.assert_called_once()

    def test_invalid_mode(self):
        with patch("sys.argv", ["run_pipeline.py", "invalid_mode"]):
            import run_pipeline

            with pytest.raises(SystemExit):
                run_pipeline.main()


# ===================================================================
# run_daily_report()
# ===================================================================


@pytest.mark.integration
class TestRunDailyReport:
    def _make_mock_db(self):
        """Create a mock Database with empty return values."""
        mock_db = MagicMock()
        mock_db.get_latest_prices_batch_with_previous.return_value = pd.DataFrame()
        mock_db.get_latest_prices.return_value = pd.DataFrame()
        mock_db.get_all_fundamentals.return_value = pd.DataFrame()
        mock_db.get_price_history_batch.return_value = pd.DataFrame()
        mock_db.get_economic_indicator_history.return_value = pd.DataFrame()
        return mock_db

    @patch("run_pipeline.Database")
    def test_writes_report_file(self, mock_db_cls, tmp_path):
        """run_daily_report creates a .md file in REPORTS_DIR."""
        mock_db_cls.return_value = self._make_mock_db()
        report_dir = tmp_path / "reports"
        report_dir.mkdir()
        with (
            patch("run_pipeline.REPORTS_DIR", report_dir),
            patch("run_pipeline.DB_PATH", tmp_path / "test.db"),
            patch("run_pipeline.WATCHLIST", {"tech": ["AAPL"]}),
            patch("run_pipeline.ECONOMIC_INDICATORS", {}),
            patch("sqlite3.connect") as mock_conn,
        ):
            mock_conn.return_value.__enter__ = lambda s: s
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            # pd.read_sql_query needs to return a DataFrame
            with patch("run_pipeline.pd.read_sql_query", return_value=pd.DataFrame()):
                import run_pipeline

                run_pipeline.run_daily_report()
        md_files = list(report_dir.glob("*.md"))
        assert len(md_files) == 1

    @patch("run_pipeline.Database")
    def test_empty_db_no_crash(self, mock_db_cls, tmp_path):
        """Empty database produces a report without crashing."""
        mock_db_cls.return_value = self._make_mock_db()
        report_dir = tmp_path / "reports"
        report_dir.mkdir()
        with (
            patch("run_pipeline.REPORTS_DIR", report_dir),
            patch("run_pipeline.DB_PATH", tmp_path / "test.db"),
            patch("run_pipeline.WATCHLIST", {}),
            patch("run_pipeline.ECONOMIC_INDICATORS", {}),
            patch("sqlite3.connect") as mock_conn,
        ):
            mock_conn.return_value.__enter__ = lambda s: s
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            with patch("run_pipeline.pd.read_sql_query", return_value=pd.DataFrame()):
                import run_pipeline

                run_pipeline.run_daily_report()
        md_files = list(report_dir.glob("*.md"))
        assert len(md_files) == 1

    @patch("run_pipeline.Database")
    def test_report_contains_markdown(self, mock_db_cls, tmp_path):
        """Generated report file contains markdown content."""
        mock_db_cls.return_value = self._make_mock_db()
        report_dir = tmp_path / "reports"
        report_dir.mkdir()
        with (
            patch("run_pipeline.REPORTS_DIR", report_dir),
            patch("run_pipeline.DB_PATH", tmp_path / "test.db"),
            patch("run_pipeline.WATCHLIST", {"tech": ["AAPL"]}),
            patch("run_pipeline.ECONOMIC_INDICATORS", {}),
            patch("sqlite3.connect") as mock_conn,
        ):
            mock_conn.return_value.__enter__ = lambda s: s
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            with patch("run_pipeline.pd.read_sql_query", return_value=pd.DataFrame()):
                import run_pipeline

                run_pipeline.run_daily_report()
        md_files = list(report_dir.glob("*.md"))
        content = md_files[0].read_text(encoding="utf-8")
        assert "#" in content  # markdown headers present
