"""Tests for src/brief_exporter.py — OpenBB data export for briefs pipeline."""

import json
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from brief_exporter import (
    _build_portfolio_snapshot,
    _build_sec_activity,
    _build_technicals,
    export_brief_data,
)

# ===================================================================
# Helpers
# ===================================================================


def _seed_db(db, symbols=("AAPL", "MSFT")):
    """Seed a test DB with prices, fundamentals, filings, and economic data."""
    for sym in symbols:
        # Prices: 30 days
        dates = pd.date_range(end=datetime.now().strftime("%Y-%m-%d"), periods=30, freq="B")
        prices = pd.DataFrame(
            {
                "date": dates.strftime("%Y-%m-%d"),
                "open": [150.0 + i for i in range(30)],
                "high": [155.0 + i for i in range(30)],
                "low": [148.0 + i for i in range(30)],
                "close": [152.0 + i for i in range(30)],
                "volume": [1_000_000 + i * 10000 for i in range(30)],
            }
        )
        db.save_prices(prices, sym)

        # Fundamentals
        fund = pd.DataFrame(
            {
                "symbol": [sym],
                "snapshot_date": [datetime.now().strftime("%Y-%m-%d")],
                "market_cap": [2_500_000_000_000],
                "pe_ratio": [28.5],
                "pb_ratio": [12.3],
                "eps": [6.5],
                "revenue": [380_000_000_000],
            }
        )
        db.save_fundamentals(fund, sym)

        # SEC filings
        filings = pd.DataFrame(
            {
                "symbol": [sym, sym],
                "filing_date": ["2026-01-15", "2026-03-01"],
                "report_type": ["10-K", "8-K"],
                "report_url": ["https://sec.gov/10k", "https://sec.gov/8k"],
                "primary_doc_description": ["Annual Report", "Current Report"],
                "accession_number": ["0001-26-000001", "0001-26-000002"],
            }
        )
        db.save_sec_filings(filings, sym)

    # Economic indicators
    for series_id in ("VIXCLS", "DGS10", "T10Y2Y"):
        dates = pd.date_range(end=datetime.now().strftime("%Y-%m-%d"), periods=12, freq="MS")
        econ = pd.DataFrame(
            {
                "date": dates.strftime("%Y-%m-%d"),
                "value": [3.5 + i * 0.1 for i in range(12)],
            }
        )
        db.save_economic_indicators(econ, series_id)


# ===================================================================
# Unit tests: _build_portfolio_snapshot
# ===================================================================


@pytest.mark.unit
class TestBuildPortfolioSnapshot:
    def test_returns_list_of_dicts(self, tmp_db):
        _seed_db(tmp_db)
        result = _build_portfolio_snapshot(tmp_db)
        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(r, dict) for r in result)

    def test_each_entry_has_required_fields(self, tmp_db):
        _seed_db(tmp_db)
        result = _build_portfolio_snapshot(tmp_db)
        for entry in result:
            assert "symbol" in entry
            assert "price" in entry
            assert "change_pct" in entry
            assert "volume" in entry
            assert "sector" in entry

    def test_empty_db_returns_empty(self, tmp_db):
        result = _build_portfolio_snapshot(tmp_db)
        assert result == []


# ===================================================================
# Unit tests: _build_technicals
# ===================================================================


@pytest.mark.unit
class TestBuildTechnicals:
    def test_returns_dict_of_dicts(self, tmp_db):
        _seed_db(tmp_db, symbols=("AAPL",))
        with patch("brief_exporter.ALL_SYMBOLS", ["AAPL"]):
            result = _build_technicals(tmp_db)
        assert isinstance(result, dict)
        assert "AAPL" in result
        assert "latest_close" in result["AAPL"]

    def test_missing_symbol_skipped(self, tmp_db):
        with patch("brief_exporter.ALL_SYMBOLS", ["ZZZZ"]):
            result = _build_technicals(tmp_db)
        assert result == {}


# ===================================================================
# Unit tests: _build_sec_activity
# ===================================================================


@pytest.mark.unit
class TestBuildSecActivity:
    def test_returns_dict(self, tmp_db):
        _seed_db(tmp_db)
        with patch("brief_exporter.ALL_SYMBOLS", ["AAPL", "MSFT"]):
            result = _build_sec_activity(days=365)
        assert isinstance(result, dict)
        assert "per_symbol" in result

    def test_empty_db_returns_structure(self, tmp_db):
        with patch("brief_exporter.ALL_SYMBOLS", ["ZZZZ"]):
            result = _build_sec_activity(days=90)
        assert isinstance(result, dict)


# ===================================================================
# Integration tests: export_brief_data
# ===================================================================


@pytest.mark.integration
class TestExportBriefData:
    def test_writes_json_file(self, tmp_db, tmp_path):
        _seed_db(tmp_db)
        output_path = tmp_path / "brief_data.json"
        with (
            patch("brief_exporter.Database", return_value=tmp_db),
            patch("brief_exporter.ALL_SYMBOLS", ["AAPL", "MSFT"]),
        ):
            export_brief_data(output_path=output_path)

        assert output_path.exists()
        data = json.loads(output_path.read_text())
        assert "generated_at" in data
        assert "date" in data

    def test_output_has_all_sections(self, tmp_db, tmp_path):
        _seed_db(tmp_db)
        output_path = tmp_path / "brief_data.json"
        with (
            patch("brief_exporter.Database", return_value=tmp_db),
            patch("brief_exporter.ALL_SYMBOLS", ["AAPL", "MSFT"]),
        ):
            result = export_brief_data(output_path=output_path)

        expected_sections = [
            "portfolio_snapshot",
            "technical_signals",
            "valuation_check",
            "risk_dashboard",
            "macro_snapshot",
            "sec_activity",
            "alerts",
        ]
        for section in expected_sections:
            assert section in result, f"Missing section: {section}"

    def test_portfolio_snapshot_has_symbols(self, tmp_db, tmp_path):
        _seed_db(tmp_db)
        output_path = tmp_path / "brief_data.json"
        with (
            patch("brief_exporter.Database", return_value=tmp_db),
            patch("brief_exporter.ALL_SYMBOLS", ["AAPL", "MSFT"]),
        ):
            result = export_brief_data(output_path=output_path)

        symbols = [s["symbol"] for s in result["portfolio_snapshot"]]
        assert "AAPL" in symbols
        assert "MSFT" in symbols

    def test_technicals_computed(self, tmp_db, tmp_path):
        _seed_db(tmp_db)
        output_path = tmp_path / "brief_data.json"
        with (
            patch("brief_exporter.Database", return_value=tmp_db),
            patch("brief_exporter.ALL_SYMBOLS", ["AAPL"]),
        ):
            result = export_brief_data(output_path=output_path)

        assert "AAPL" in result["technical_signals"]
        tech = result["technical_signals"]["AAPL"]
        assert "latest_close" in tech
        assert "price_vs_sma20" in tech

    def test_json_is_valid(self, tmp_db, tmp_path):
        """Ensure the JSON file is parseable and has no NaN values."""
        _seed_db(tmp_db)
        output_path = tmp_path / "brief_data.json"
        with (
            patch("brief_exporter.Database", return_value=tmp_db),
            patch("brief_exporter.ALL_SYMBOLS", ["AAPL"]),
        ):
            export_brief_data(output_path=output_path)

        raw = output_path.read_text()
        assert "NaN" not in raw
        assert "Infinity" not in raw
        data = json.loads(raw)
        assert isinstance(data, dict)

    def test_generated_at_is_iso_format(self, tmp_db, tmp_path):
        _seed_db(tmp_db)
        output_path = tmp_path / "brief_data.json"
        with (
            patch("brief_exporter.Database", return_value=tmp_db),
            patch("brief_exporter.ALL_SYMBOLS", ["AAPL"]),
        ):
            result = export_brief_data(output_path=output_path)

        # Should parse without error
        datetime.fromisoformat(result["generated_at"])

    def test_empty_db_still_produces_valid_json(self, tmp_db, tmp_path):
        """Export with no data should not crash."""
        output_path = tmp_path / "brief_data.json"
        with (
            patch("brief_exporter.Database", return_value=tmp_db),
            patch("brief_exporter.ALL_SYMBOLS", []),
        ):
            result = export_brief_data(output_path=output_path)

        assert output_path.exists()
        assert result["portfolio_snapshot"] == []

    def test_alerts_are_list(self, tmp_db, tmp_path):
        _seed_db(tmp_db)
        output_path = tmp_path / "brief_data.json"
        with (
            patch("brief_exporter.Database", return_value=tmp_db),
            patch("brief_exporter.ALL_SYMBOLS", ["AAPL"]),
        ):
            result = export_brief_data(output_path=output_path)

        assert isinstance(result["alerts"], list)

    def test_macro_snapshot_has_indicators(self, tmp_db, tmp_path):
        _seed_db(tmp_db)
        output_path = tmp_path / "brief_data.json"
        with (
            patch("brief_exporter.Database", return_value=tmp_db),
            patch("brief_exporter.ALL_SYMBOLS", ["AAPL"]),
        ):
            result = export_brief_data(output_path=output_path)

        macro = result["macro_snapshot"]
        assert "indicators" in macro
        assert isinstance(macro["indicators"], list)
