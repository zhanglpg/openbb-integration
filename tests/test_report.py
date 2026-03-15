"""Unit tests for src/report.py — daily report generator."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from report import (  # noqa: I001, E402
    format_report_markdown,
    generate_daily_report,
    identify_alerts,
    identify_notable_movers,
)


# ===================================================================
# Helpers
# ===================================================================


def _portfolio_overview():
    return [
        {"symbol": "SPY", "sector": "etfs", "price": 450.0, "change_pct": 1.2},
        {"symbol": "QQQ", "sector": "etfs", "price": 380.0, "change_pct": -0.5},
        {"symbol": "AAPL", "sector": "tech", "price": 190.0, "change_pct": 3.5},
        {"symbol": "MSFT", "sector": "tech", "price": 420.0, "change_pct": -4.0},
        {"symbol": "NVDA", "sector": "semiconductors", "price": 800.0, "change_pct": 0.3},
        {"symbol": "BABA", "sector": "china", "price": 85.0, "change_pct": None},
    ]


def _technicals():
    return {
        "AAPL": {
            "symbol": "AAPL",
            "latest_close": 190.0,
            "sma_5": 192.0,
            "sma_10": 188.0,
            "sma_20": 185.0,
            "price_vs_sma20": "above",
            "daily_volatility": 0.015,
            "max_drawdown_pct": -5.0,
            "avg_volume": 50_000_000,
            "volume_trend_ratio": 1.2,
            "total_return_pct": 8.5,
        },
        "MSFT": {
            "symbol": "MSFT",
            "latest_close": 420.0,
            "sma_5": 415.0,
            "sma_10": 418.0,
            "sma_20": 425.0,
            "price_vs_sma20": "below",
            "daily_volatility": 0.012,
            "max_drawdown_pct": -8.0,
            "avg_volume": 30_000_000,
            "volume_trend_ratio": 2.5,
            "total_return_pct": -3.0,
        },
        "NVDA": {
            "symbol": "NVDA",
            "latest_close": 800.0,
            "sma_5": 790.0,
            "sma_10": 780.0,
            "sma_20": 750.0,
            "price_vs_sma20": "above",
            "daily_volatility": 0.025,
            "max_drawdown_pct": -18.0,
            "avg_volume": 40_000_000,
            "volume_trend_ratio": 1.0,
            "total_return_pct": 15.0,
        },
    }


def _macro_snapshot():
    return {
        "indicators": [
            {"series_id": "VIXCLS", "latest_value": 28.0, "change_1m": 5.0},
            {"series_id": "T10Y2Y", "latest_value": -0.3, "change_1m": -0.1},
        ],
        "yield_curve_status": "inverted",
        "vix_regime": "high",
        "rate_direction": "stable",
    }


def _risk_summary():
    return {
        "per_symbol": [],
        "portfolio": {
            "avg_pairwise_correlation": 0.65,
            "sector_concentration": {"tech": 50.0, "china": 25.0, "etfs": 25.0},
        },
        "most_volatile_3": ["NVDA", "AAPL", "MSFT"],
        "least_volatile_3": ["SPY", "QQQ", "BABA"],
    }


def _sec_activity():
    return {
        "per_symbol": [],
        "recent_8k_activity": [
            {"symbol": "AAPL", "date": "2026-03-10", "description": "Earnings release"},
            {"symbol": "MSFT", "date": "2026-03-08", "description": "Officer change"},
        ],
        "inactive_symbols": ["BIDU"],
    }


def _valuations():
    return [
        {"symbol": "AAPL", "pe_ratio": 30.0, "pb_ratio": 12.0},
        {"symbol": "MSFT", "pe_ratio": 35.0, "pb_ratio": 14.0},
    ]


# ===================================================================
# identify_notable_movers
# ===================================================================


@pytest.mark.unit
class TestIdentifyNotableMovers:
    def test_finds_movers_above_threshold(self):
        result = identify_notable_movers(_portfolio_overview(), threshold_pct=3.0)
        symbols = [m["symbol"] for m in result]
        assert "AAPL" in symbols  # +3.5%
        assert "MSFT" in symbols  # -4.0%

    def test_excludes_below_threshold(self):
        result = identify_notable_movers(_portfolio_overview(), threshold_pct=3.0)
        symbols = [m["symbol"] for m in result]
        assert "SPY" not in symbols  # +1.2%
        assert "NVDA" not in symbols  # +0.3%

    def test_excludes_none_change(self):
        result = identify_notable_movers(_portfolio_overview(), threshold_pct=0.1)
        symbols = [m["symbol"] for m in result]
        assert "BABA" not in symbols

    def test_sorted_by_absolute_change(self):
        result = identify_notable_movers(_portfolio_overview(), threshold_pct=3.0)
        abs_changes = [abs(m["change_pct"]) for m in result]
        assert abs_changes == sorted(abs_changes, reverse=True)

    def test_custom_threshold(self):
        result = identify_notable_movers(_portfolio_overview(), threshold_pct=1.0)
        symbols = [m["symbol"] for m in result]
        assert "SPY" in symbols  # +1.2%

    def test_empty_input(self):
        assert identify_notable_movers([]) == []


# ===================================================================
# identify_alerts
# ===================================================================


@pytest.mark.unit
class TestIdentifyAlerts:
    def test_sma_crossover_bullish(self):
        # AAPL: sma_5 (192) > sma_20 (185) * 1.02 = 188.7 → bullish
        alerts = identify_alerts(_technicals(), {}, {})
        bullish = [a for a in alerts if "bullish" in a["message"].lower()]
        assert any("AAPL" in a["message"] for a in bullish)

    def test_sma_crossover_bearish(self):
        # MSFT: sma_5 (415) < sma_20 (425) * 0.98 = 416.5 → bearish
        alerts = identify_alerts(_technicals(), {}, {})
        bearish = [a for a in alerts if "bearish" in a["message"].lower()]
        assert any("MSFT" in a["message"] for a in bearish)

    def test_volume_surge_alert(self):
        alerts = identify_alerts(_technicals(), {}, {})
        vol_alerts = [a for a in alerts if a["category"] == "volume"]
        assert any("MSFT" in a["message"] for a in vol_alerts)  # vol_ratio=2.5

    def test_drawdown_alert(self):
        alerts = identify_alerts(_technicals(), {}, {})
        risk_alerts = [a for a in alerts if a["category"] == "risk"]
        assert any("NVDA" in a["message"] for a in risk_alerts)  # drawdown=-18%

    def test_yield_curve_inverted_alert(self):
        alerts = identify_alerts({}, _macro_snapshot(), {})
        macro_alerts = [a for a in alerts if a["category"] == "macro"]
        assert any("yield curve" in a["message"].lower() for a in macro_alerts)

    def test_vix_high_alert(self):
        alerts = identify_alerts({}, _macro_snapshot(), {})
        macro_alerts = [a for a in alerts if a["category"] == "macro"]
        assert any("vix" in a["message"].lower() for a in macro_alerts)

    def test_no_alerts_for_calm_market(self):
        calm_tech = {
            "SPY": {
                "symbol": "SPY",
                "sma_5": 450.0,
                "sma_20": 448.0,  # within 2% band
                "volume_trend_ratio": 1.0,
                "max_drawdown_pct": -2.0,
            }
        }
        calm_macro = {"yield_curve_status": "normal", "vix_regime": "low"}
        alerts = identify_alerts(calm_tech, calm_macro, {})
        assert len(alerts) == 0

    def test_many_8k_alert(self):
        sec = {"recent_8k_activity": [{"symbol": f"S{i}"} for i in range(10)]}
        alerts = identify_alerts({}, {}, sec)
        assert any("8-K" in a["message"] for a in alerts)


# ===================================================================
# generate_daily_report
# ===================================================================


@pytest.mark.unit
class TestGenerateDailyReport:
    def _make_report(self):
        return generate_daily_report(
            portfolio_overview=_portfolio_overview(),
            technicals=_technicals(),
            valuations=_valuations(),
            risk_summary=_risk_summary(),
            macro_snapshot=_macro_snapshot(),
            sec_activity=_sec_activity(),
            report_date="2026-03-15",
        )

    def test_report_has_date(self):
        report = self._make_report()
        assert report["date"] == "2026-03-15"

    def test_report_has_benchmarks(self):
        report = self._make_report()
        syms = [b["symbol"] for b in report["benchmarks"]]
        assert "SPY" in syms
        assert "QQQ" in syms

    def test_top_performers(self):
        report = self._make_report()
        assert len(report["top_performers"]) == 3
        assert report["top_performers"][0]["symbol"] == "AAPL"  # +3.5%

    def test_bottom_performers(self):
        report = self._make_report()
        assert report["bottom_performers"][-1]["symbol"] == "MSFT"  # -4.0%

    def test_notable_movers_included(self):
        report = self._make_report()
        assert len(report["notable_movers"]) >= 1

    def test_technical_signals(self):
        report = self._make_report()
        assert len(report["technical_signals"]) == 3

    def test_macro_summary(self):
        report = self._make_report()
        assert report["macro_summary"]["yield_curve"] == "inverted"
        assert report["macro_summary"]["vix_regime"] == "high"

    def test_alerts_included(self):
        report = self._make_report()
        assert len(report["alerts"]) > 0

    def test_risk_highlights(self):
        report = self._make_report()
        assert report["risk_highlights"]["most_volatile"] == ["NVDA", "AAPL", "MSFT"]


# ===================================================================
# format_report_markdown
# ===================================================================


@pytest.mark.unit
class TestFormatReportMarkdown:
    def test_contains_title(self):
        report = generate_daily_report(
            _portfolio_overview(),
            _technicals(),
            _valuations(),
            _risk_summary(),
            _macro_snapshot(),
            _sec_activity(),
            report_date="2026-03-15",
        )
        md = format_report_markdown(report)
        assert "# Daily Market Report" in md
        assert "2026-03-15" in md

    def test_contains_sections(self):
        report = generate_daily_report(
            _portfolio_overview(),
            _technicals(),
            _valuations(),
            _risk_summary(),
            _macro_snapshot(),
            _sec_activity(),
        )
        md = format_report_markdown(report)
        assert "## Market Summary" in md
        assert "## Top Performers" in md
        assert "## Macro Environment" in md
        assert "## Alerts" in md

    def test_contains_benchmark_prices(self):
        report = generate_daily_report(
            _portfolio_overview(),
            _technicals(),
            _valuations(),
            _risk_summary(),
            _macro_snapshot(),
            _sec_activity(),
        )
        md = format_report_markdown(report)
        assert "SPY" in md
        assert "$450" in md

    def test_empty_report_still_formats(self):
        report = generate_daily_report([], {}, [], {}, {}, {}, report_date="2026-01-01")
        md = format_report_markdown(report)
        assert "# Daily Market Report" in md
        assert "2026-01-01" in md
