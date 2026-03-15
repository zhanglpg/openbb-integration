"""Unit tests for src/research.py — composite research functions."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from research import (  # noqa: I001, E402
    analyze_symbol_deep,
    assess_macro_risks,
    compare_peers,
    screen_opportunities,
)


# ===================================================================
# Helpers
# ===================================================================


def _technicals():
    return {
        "NVDA": {
            "symbol": "NVDA",
            "total_return_pct": 15.0,
            "daily_volatility": 0.025,
            "max_drawdown_pct": -12.0,
            "price_vs_sma20": "above",
            "volume_trend_ratio": 1.3,
            "sma_5": 800.0,
            "sma_20": 750.0,
            "latest_close": 810.0,
        },
        "AMD": {
            "symbol": "AMD",
            "total_return_pct": -5.0,
            "daily_volatility": 0.030,
            "max_drawdown_pct": -20.0,
            "price_vs_sma20": "below",
            "volume_trend_ratio": 0.9,
            "sma_5": 140.0,
            "sma_20": 155.0,
            "latest_close": 138.0,
        },
        "INTC": {
            "symbol": "INTC",
            "total_return_pct": -8.0,
            "daily_volatility": 0.020,
            "max_drawdown_pct": -15.0,
            "price_vs_sma20": "below",
            "volume_trend_ratio": 1.1,
            "sma_5": 22.0,
            "sma_20": 25.0,
            "latest_close": 21.0,
        },
    }


def _fundamentals():
    return [
        {
            "symbol": "NVDA",
            "pe_ratio": 60.0,
            "pb_ratio": 30.0,
            "market_cap": 2e12,
            "eps": 12.0,
            "revenue": 60e9,
            "free_cash_flow": 20e9,
            "fcf_yield": 0.01,
        },
        {
            "symbol": "AMD",
            "pe_ratio": 25.0,
            "pb_ratio": 4.0,
            "market_cap": 220e9,
            "eps": 4.0,
            "revenue": 23e9,
            "free_cash_flow": 3e9,
            "fcf_yield": 0.014,
        },
        {
            "symbol": "INTC",
            "pe_ratio": 12.0,
            "pb_ratio": 1.2,
            "market_cap": 90e9,
            "eps": 1.5,
            "revenue": 55e9,
            "free_cash_flow": 5e9,
            "fcf_yield": 0.056,
        },
    ]


def _risk_data():
    return {
        "per_symbol": [
            {
                "symbol": "NVDA",
                "daily_volatility": 0.025,
                "sharpe_proxy": 1.5,
                "sector": "semiconductors",
            },
            {
                "symbol": "AMD",
                "daily_volatility": 0.030,
                "sharpe_proxy": -0.3,
                "sector": "semiconductors",
            },
            {
                "symbol": "INTC",
                "daily_volatility": 0.020,
                "sharpe_proxy": -0.8,
                "sector": "semiconductors",
            },
        ],
        "portfolio": {
            "avg_pairwise_correlation": 0.75,
            "sector_concentration": {"semiconductors": 100.0},
        },
        "most_volatile_3": ["AMD", "NVDA", "INTC"],
        "least_volatile_3": ["INTC", "NVDA", "AMD"],
    }


# ===================================================================
# compare_peers
# ===================================================================


@pytest.mark.unit
class TestComparePeers:
    def test_basic_structure(self):
        result = compare_peers(
            ["NVDA", "AMD", "INTC"], _technicals(), _fundamentals(), _risk_data()
        )
        assert result["symbols"] == ["NVDA", "AMD", "INTC"]
        assert len(result["technicals"]) == 3
        assert len(result["fundamentals"]) == 3

    def test_return_ranking(self):
        result = compare_peers(
            ["NVDA", "AMD", "INTC"], _technicals(), _fundamentals(), _risk_data()
        )
        assert result["rankings"]["by_return"][0] == "NVDA"  # +15%

    def test_volatility_ranking(self):
        result = compare_peers(
            ["NVDA", "AMD", "INTC"], _technicals(), _fundamentals(), _risk_data()
        )
        assert result["rankings"]["by_volatility_asc"][0] == "INTC"  # lowest vol

    def test_pe_ranking(self):
        result = compare_peers(
            ["NVDA", "AMD", "INTC"], _technicals(), _fundamentals(), _risk_data()
        )
        assert result["rankings"]["by_pe_asc"][0] == "INTC"  # PE=12

    def test_subset_of_symbols(self):
        result = compare_peers(["NVDA", "AMD"], _technicals(), _fundamentals(), _risk_data())
        assert len(result["technicals"]) == 2
        assert len(result["fundamentals"]) == 2

    def test_empty_symbols(self):
        result = compare_peers([], _technicals(), _fundamentals(), _risk_data())
        assert result["technicals"] == []
        assert result["fundamentals"] == []


# ===================================================================
# analyze_symbol_deep
# ===================================================================


@pytest.mark.unit
class TestAnalyzeSymbolDeep:
    def test_basic_output(self):
        result = analyze_symbol_deep(
            "NVDA",
            _technicals()["NVDA"],
            _fundamentals()[0],
            [{"report_type": "10-K", "filing_date": "2026-01-15"}],
        )
        assert result["symbol"] == "NVDA"
        assert "technical_summary" in result
        assert "fundamental_summary" in result
        assert "sec_summary" in result

    def test_technical_summary(self):
        result = analyze_symbol_deep("NVDA", _technicals()["NVDA"], {}, [])
        assert result["technical_summary"]["trend"] == "above"
        assert result["technical_summary"]["total_return_pct"] == 15.0

    def test_fundamental_summary(self):
        result = analyze_symbol_deep("NVDA", {}, _fundamentals()[0], [])
        assert result["fundamental_summary"]["pe_ratio"] == 60.0

    def test_sec_summary(self):
        filings = [
            {"report_type": "10-K"},
            {"report_type": "8-K"},
            {"report_type": "8-K"},
        ]
        result = analyze_symbol_deep("NVDA", {}, {}, filings)
        assert result["sec_summary"]["total_filings"] == 3
        assert result["sec_summary"]["by_type"]["8-K"] == 2

    def test_peer_position(self):
        peer_ctx = compare_peers(
            ["NVDA", "AMD", "INTC"], _technicals(), _fundamentals(), _risk_data()
        )
        result = analyze_symbol_deep(
            "NVDA", _technicals()["NVDA"], _fundamentals()[0], [], peer_context=peer_ctx
        )
        assert result["peer_position"]["by_return"]["rank"] == 1

    def test_signals_above_sma(self):
        result = analyze_symbol_deep("NVDA", _technicals()["NVDA"], {}, [])
        assert any("above SMA-20" in s for s in result["signals"])

    def test_signals_below_sma(self):
        result = analyze_symbol_deep("AMD", _technicals()["AMD"], {}, [])
        assert any("below SMA-20" in s for s in result["signals"])

    def test_signals_high_pe(self):
        result = analyze_symbol_deep("NVDA", {}, _fundamentals()[0], [])
        assert any("High PE" in s for s in result["signals"])

    def test_signals_low_pe(self):
        result = analyze_symbol_deep("INTC", {}, _fundamentals()[2], [])
        assert any("Low PE" in s for s in result["signals"])

    def test_error_technicals(self):
        result = analyze_symbol_deep("X", {"error": "no data"}, {}, [])
        assert result["technical_summary"]["error"] == "no data"


# ===================================================================
# assess_macro_risks
# ===================================================================


@pytest.mark.unit
class TestAssessMacroRisks:
    def test_inverted_yield_curve(self):
        macro = {"yield_curve_status": "inverted", "vix_regime": "low", "rate_direction": "stable"}
        result = assess_macro_risks(macro, _risk_data())
        assert result["overall_risk_level"] == "elevated"
        factors = [c["factor"] for c in result["concerns"]]
        assert "yield_curve" in factors

    def test_high_vix(self):
        macro = {"yield_curve_status": "normal", "vix_regime": "high", "rate_direction": "stable"}
        result = assess_macro_risks(macro, _risk_data())
        assert result["overall_risk_level"] == "high"

    def test_rising_rates(self):
        macro = {"yield_curve_status": "normal", "vix_regime": "low", "rate_direction": "rising"}
        result = assess_macro_risks(macro, _risk_data())
        assert result["overall_risk_level"] == "moderate"

    def test_concentration_concern(self):
        result = assess_macro_risks(
            {"yield_curve_status": "normal", "vix_regime": "low", "rate_direction": "stable"},
            _risk_data(),
        )
        factors = [c["factor"] for c in result["concerns"]]
        assert "concentration" in factors  # semiconductors at 100%

    def test_high_correlation_concern(self):
        result = assess_macro_risks(
            {"yield_curve_status": "normal", "vix_regime": "low", "rate_direction": "stable"},
            _risk_data(),
        )
        factors = [c["factor"] for c in result["concerns"]]
        assert "correlation" in factors  # avg_corr=0.75

    def test_calm_market(self):
        macro = {"yield_curve_status": "normal", "vix_regime": "low", "rate_direction": "stable"}
        risk = {
            "portfolio": {
                "avg_pairwise_correlation": 0.3,
                "sector_concentration": {"tech": 30.0, "china": 30.0, "etfs": 40.0},
            },
            "most_volatile_3": [],
        }
        result = assess_macro_risks(macro, risk)
        assert result["overall_risk_level"] == "low"
        assert len(result["concerns"]) == 0


# ===================================================================
# screen_opportunities
# ===================================================================


@pytest.mark.unit
class TestScreenOpportunities:
    def test_finds_oversold_value(self):
        result = screen_opportunities(_fundamentals(), _technicals(), max_pe=30.0)
        symbols = [o["symbol"] for o in result]
        # INTC: PE=12, below SMA-20, down -8% → should score well
        assert "INTC" in symbols

    def test_excludes_high_pe(self):
        result = screen_opportunities(_fundamentals(), _technicals(), max_pe=30.0)
        symbols = [o["symbol"] for o in result]
        assert "NVDA" not in symbols  # PE=60

    def test_sorted_by_score_desc(self):
        result = screen_opportunities(_fundamentals(), _technicals(), max_pe=30.0)
        scores = [o["score"] for o in result]
        assert scores == sorted(scores, reverse=True)

    def test_reasons_populated(self):
        result = screen_opportunities(_fundamentals(), _technicals(), max_pe=30.0)
        for opp in result:
            assert len(opp["reasons"]) > 0

    def test_custom_max_pe(self):
        result = screen_opportunities(_fundamentals(), _technicals(), max_pe=100.0)
        # NVDA now eligible (PE=60 < 100) — verify no crash with wider filter
        assert isinstance(result, list)

    def test_empty_inputs(self):
        assert screen_opportunities([], {}) == []

    def test_negative_pe_excluded(self):
        vals = [{"symbol": "X", "pe_ratio": -5.0}]
        techs = {
            "X": {
                "price_vs_sma20": "below",
                "total_return_pct": -10.0,
                "max_drawdown_pct": -5.0,
            }
        }
        assert screen_opportunities(vals, techs) == []
