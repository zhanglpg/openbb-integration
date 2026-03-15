"""Unit tests for src/analysis.py — pure analysis functions."""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Ensure src/ is importable (conftest.py also does this)
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from analysis import (  # noqa: I001, E402
    compute_bollinger_bands,
    compute_macd,
    compute_macro_snapshot,
    compute_portfolio_risk,
    compute_price_technicals,
    compute_sec_activity,
    compute_valuation_screen,
    resample_ohlcv,
)


# ===================================================================
# Helpers
# ===================================================================


def _make_price_df(closes, start="2025-01-01", volumes=None):
    """Build an OHLCV DataFrame from a list of close prices."""
    dates = pd.bdate_range(start, periods=len(closes))
    df = pd.DataFrame(
        {
            "date": dates.strftime("%Y-%m-%d"),
            "open": [c - 1 for c in closes],
            "high": [c + 1 for c in closes],
            "low": [c - 2 for c in closes],
            "close": closes,
            "volume": volumes or [1_000_000] * len(closes),
        }
    )
    return df


# ===================================================================
# compute_price_technicals
# ===================================================================


@pytest.mark.unit
class TestComputePriceTechnicals:
    def test_basic_output_keys(self):
        df = _make_price_df([100, 101, 102, 103, 104])
        result = compute_price_technicals(df, "AAPL")
        expected_keys = {
            "symbol",
            "latest_close",
            "sma_5",
            "sma_10",
            "sma_20",
            "price_vs_sma20",
            "daily_volatility",
            "max_drawdown_pct",
            "avg_volume",
            "volume_trend_ratio",
            "high_low_range_pct",
            "total_return_pct",
        }
        assert expected_keys == set(result.keys())

    def test_latest_close(self):
        df = _make_price_df([100, 110, 120])
        result = compute_price_technicals(df, "TEST")
        assert result["latest_close"] == 120

    def test_sma_5_with_5_points(self):
        closes = [10, 20, 30, 40, 50]
        df = _make_price_df(closes)
        result = compute_price_technicals(df, "TEST")
        assert result["sma_5"] == 30.0  # mean of [10,20,30,40,50]

    def test_sma_none_when_insufficient_data(self):
        df = _make_price_df([100, 101])
        result = compute_price_technicals(df, "TEST")
        assert result["sma_5"] is None
        assert result["sma_10"] is None
        assert result["sma_20"] is None

    def test_price_vs_sma20(self):
        # 20 ascending prices: last > mean → above
        closes = list(range(100, 120))
        df = _make_price_df(closes)
        result = compute_price_technicals(df, "TEST")
        assert result["price_vs_sma20"] == "above"

    def test_total_return(self):
        df = _make_price_df([100, 110])
        result = compute_price_technicals(df, "TEST")
        assert result["total_return_pct"] == 10.0

    def test_max_drawdown_no_drawdown(self):
        df = _make_price_df([100, 101, 102, 103])
        result = compute_price_technicals(df, "TEST")
        assert result["max_drawdown_pct"] == 0.0

    def test_max_drawdown_with_dip(self):
        df = _make_price_df([100, 90, 95])
        result = compute_price_technicals(df, "TEST")
        assert result["max_drawdown_pct"] == -10.0

    def test_empty_df(self):
        df = pd.DataFrame()
        result = compute_price_technicals(df, "TEST")
        assert result["error"] == "insufficient data"

    def test_single_data_point(self):
        df = _make_price_df([100])
        result = compute_price_technicals(df, "TEST")
        assert result["latest_close"] == 100
        assert result["daily_volatility"] is None

    def test_volume_trend_ratio(self):
        # 20 days: first 15 with vol=1M, last 5 with vol=2M
        closes = [100] * 20
        volumes = [1_000_000] * 15 + [2_000_000] * 5
        df = _make_price_df(closes, volumes=volumes)
        result = compute_price_technicals(df, "TEST")
        # 5d avg = 2M, 20d avg = (15*1M + 5*2M)/20 = 1.25M → ratio = 1.6
        assert result["volume_trend_ratio"] == 1.6


# ===================================================================
# compute_valuation_screen
# ===================================================================


@pytest.mark.unit
class TestComputeValuationScreen:
    def _make_fundamentals(self):
        return pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT", "GOOGL"],
                "market_cap": [3e12, 2.8e12, 2e12],
                "pe_ratio": [30.0, 35.0, 25.0],
                "pb_ratio": [12.0, 14.0, 6.0],
                "free_cash_flow": [100e9, 70e9, 80e9],
                "eps": [6.5, 11.0, 5.5],
            }
        )

    def test_sorted_by_pe(self):
        result = compute_valuation_screen(self._make_fundamentals(), sort_by="pe_ratio")
        pes = [r["pe_ratio"] for r in result]
        assert pes == sorted(pes)

    def test_fcf_yield_computed(self):
        result = compute_valuation_screen(self._make_fundamentals())
        aapl = [r for r in result if r["symbol"] == "AAPL"][0]
        expected = round(100e9 / 3e12, 4)
        assert aapl["fcf_yield"] == expected

    def test_earnings_yield_computed(self):
        result = compute_valuation_screen(self._make_fundamentals())
        googl = [r for r in result if r["symbol"] == "GOOGL"][0]
        assert googl["earnings_yield"] == round(1 / 25.0, 4)

    def test_empty_df(self):
        assert compute_valuation_screen(pd.DataFrame()) == []

    def test_missing_fcf_column(self):
        df = pd.DataFrame({"symbol": ["A"], "pe_ratio": [20.0], "market_cap": [1e9]})
        result = compute_valuation_screen(df)
        assert result[0]["fcf_yield"] is None

    def test_drops_internal_columns(self):
        df = self._make_fundamentals()
        df["id"] = [1, 2, 3]
        df["fetched_at"] = ["2025-01-01"] * 3
        result = compute_valuation_screen(df)
        assert "id" not in result[0]
        assert "fetched_at" not in result[0]

    def test_pe_zero_earnings_yield(self):
        df = pd.DataFrame({"symbol": ["X"], "pe_ratio": [0.0], "market_cap": [1e9]})
        result = compute_valuation_screen(df)
        assert result[0]["earnings_yield"] is None


# ===================================================================
# compute_portfolio_risk
# ===================================================================


@pytest.mark.unit
class TestComputePortfolioRisk:
    def _make_multi_prices(self):
        dates = pd.bdate_range("2025-01-01", periods=30)
        rows = []
        np.random.seed(42)
        for sym, base in [("AAPL", 150), ("MSFT", 400), ("GOOGL", 170)]:
            prices = base + np.cumsum(np.random.randn(30) * 2)
            for d, p in zip(dates, prices):
                rows.append({"symbol": sym, "date": d.strftime("%Y-%m-%d"), "close": float(p)})
        return pd.DataFrame(rows)

    def _watchlist(self):
        return {"tech": ["AAPL", "MSFT", "GOOGL"]}

    def test_per_symbol_count(self):
        result = compute_portfolio_risk(self._make_multi_prices(), self._watchlist())
        assert len(result["per_symbol"]) == 3

    def test_per_symbol_keys(self):
        result = compute_portfolio_risk(self._make_multi_prices(), self._watchlist())
        expected_keys = {
            "symbol",
            "mean_daily_return",
            "daily_volatility",
            "sharpe_proxy",
            "sector",
        }
        assert set(result["per_symbol"][0].keys()) == expected_keys

    def test_most_least_volatile(self):
        result = compute_portfolio_risk(self._make_multi_prices(), self._watchlist())
        assert len(result["most_volatile_3"]) == 3
        assert len(result["least_volatile_3"]) == 3

    def test_avg_pairwise_correlation(self):
        result = compute_portfolio_risk(self._make_multi_prices(), self._watchlist())
        corr = result["portfolio"]["avg_pairwise_correlation"]
        assert corr is not None
        assert -1 <= corr <= 1

    def test_sector_concentration(self):
        result = compute_portfolio_risk(self._make_multi_prices(), self._watchlist())
        conc = result["portfolio"]["sector_concentration"]
        assert "tech" in conc
        assert conc["tech"] == 100.0  # all in one sector

    def test_empty_df(self):
        result = compute_portfolio_risk(pd.DataFrame(), {"tech": ["AAPL"]})
        assert result["per_symbol"] == []

    def test_single_symbol(self):
        dates = pd.bdate_range("2025-01-01", periods=10)
        df = pd.DataFrame(
            {
                "symbol": ["AAPL"] * 10,
                "date": dates.strftime("%Y-%m-%d"),
                "close": list(range(100, 110)),
            }
        )
        result = compute_portfolio_risk(df, {"tech": ["AAPL"]})
        assert len(result["per_symbol"]) == 1
        # Only 1 symbol → no pairwise correlation
        assert result["portfolio"]["avg_pairwise_correlation"] is None


# ===================================================================
# compute_macro_snapshot
# ===================================================================


@pytest.mark.unit
class TestComputeMacroSnapshot:
    def _make_histories(self):
        dates = pd.date_range("2024-01-01", periods=12, freq="MS")
        return {
            "FEDFUNDS": pd.DataFrame({"date": dates, "value": [5.5] * 6 + [5.25] * 6}),
            "T10Y2Y": pd.DataFrame({"date": dates, "value": [-0.5] * 12}),
            "VIXCLS": pd.DataFrame({"date": dates, "value": [14.0] * 12}),
            "UNRATE": pd.DataFrame({"date": dates, "value": [3.5 + i * 0.1 for i in range(12)]}),
        }

    def test_indicator_count(self):
        result = compute_macro_snapshot(self._make_histories())
        assert len(result["indicators"]) == 4

    def test_indicator_keys(self):
        result = compute_macro_snapshot(self._make_histories())
        ind = result["indicators"][0]
        expected = {
            "series_id",
            "latest_value",
            "latest_date",
            "change_1m",
            "change_3m",
            "change_6m",
            "change_1y",
        }
        assert set(ind.keys()) == expected

    def test_yield_curve_inverted(self):
        result = compute_macro_snapshot(self._make_histories())
        assert result["yield_curve_status"] == "inverted"

    def test_yield_curve_normal(self):
        dates = pd.date_range("2024-01-01", periods=3, freq="MS")
        hist = {"T10Y2Y": pd.DataFrame({"date": dates, "value": [0.5, 0.6, 0.7]})}
        result = compute_macro_snapshot(hist)
        assert result["yield_curve_status"] == "normal"

    def test_vix_low(self):
        result = compute_macro_snapshot(self._make_histories())
        assert result["vix_regime"] == "low"

    def test_vix_high(self):
        dates = pd.date_range("2024-01-01", periods=3, freq="MS")
        hist = {"VIXCLS": pd.DataFrame({"date": dates, "value": [30, 32, 35]})}
        result = compute_macro_snapshot(hist)
        assert result["vix_regime"] == "high"

    def test_rate_direction_stable(self):
        result = compute_macro_snapshot(self._make_histories())
        assert result["rate_direction"] == "stable"

    def test_rate_direction_rising(self):
        dates = pd.date_range("2024-01-01", periods=3, freq="MS")
        hist = {"FEDFUNDS": pd.DataFrame({"date": dates, "value": [4.0, 4.5, 5.0]})}
        result = compute_macro_snapshot(hist)
        assert result["rate_direction"] == "rising"

    def test_empty_input(self):
        result = compute_macro_snapshot({})
        assert result["indicators"] == []
        assert result["yield_curve_status"] is None

    def test_single_data_point(self):
        hist = {"GDP": pd.DataFrame({"date": ["2025-01-01"], "value": [28000]})}
        result = compute_macro_snapshot(hist)
        assert len(result["indicators"]) == 1
        assert result["indicators"][0]["change_1m"] is None


# ===================================================================
# compute_sec_activity
# ===================================================================


@pytest.mark.unit
class TestComputeSecActivity:
    def _make_filings(self):
        now = datetime.now()
        return pd.DataFrame(
            {
                "symbol": ["AAPL", "AAPL", "MSFT", "GOOGL"],
                "filing_date": [
                    (now - timedelta(days=10)).strftime("%Y-%m-%d"),
                    (now - timedelta(days=30)).strftime("%Y-%m-%d"),
                    (now - timedelta(days=5)).strftime("%Y-%m-%d"),
                    (now - timedelta(days=200)).strftime("%Y-%m-%d"),  # outside 90d window
                ],
                "report_type": ["10-K", "8-K", "10-Q", "10-K"],
                "primary_doc_description": [
                    "Annual Report",
                    "Current Report",
                    "Quarterly",
                    "Annual",
                ],
            }
        )

    def test_per_symbol_within_window(self):
        result = compute_sec_activity(self._make_filings(), days=90)
        symbols = {s["symbol"] for s in result["per_symbol"]}
        assert "AAPL" in symbols
        assert "MSFT" in symbols
        # GOOGL filed 200 days ago, outside 90d window
        assert "GOOGL" not in symbols

    def test_inactive_symbols(self):
        result = compute_sec_activity(self._make_filings(), days=90)
        assert "GOOGL" in result["inactive_symbols"]

    def test_recent_8k(self):
        result = compute_sec_activity(self._make_filings(), days=90)
        assert len(result["recent_8k_activity"]) == 1
        assert result["recent_8k_activity"][0]["symbol"] == "AAPL"

    def test_by_type_count(self):
        result = compute_sec_activity(self._make_filings(), days=90)
        aapl = [s for s in result["per_symbol"] if s["symbol"] == "AAPL"][0]
        assert aapl["by_type"]["10-K"] == 1
        assert aapl["by_type"]["8-K"] == 1

    def test_empty_df(self):
        result = compute_sec_activity(pd.DataFrame(), days=90)
        assert result["per_symbol"] == []
        assert result["recent_8k_activity"] == []

    def test_missing_filing_date(self):
        df = pd.DataFrame({"symbol": ["X"], "report_type": ["10-K"]})
        result = compute_sec_activity(df, days=90)
        assert result["per_symbol"] == []

    def test_days_since_last(self):
        result = compute_sec_activity(self._make_filings(), days=90)
        msft = [s for s in result["per_symbol"] if s["symbol"] == "MSFT"][0]
        assert msft["days_since_last"] >= 4  # filed ~5 days ago


# ===================================================================
# compute_bollinger_bands
# ===================================================================


@pytest.mark.unit
class TestComputeBollingerBands:
    def test_output_columns(self):
        df = _make_price_df(list(range(100, 130)))
        result = compute_bollinger_bands(df)
        assert list(result.columns) == ["date", "bb_middle", "bb_upper", "bb_lower"]

    def test_nan_count(self):
        df = _make_price_df(list(range(100, 130)))
        result = compute_bollinger_bands(df, window=20)
        # First 19 rows should be NaN (window - 1)
        assert result["bb_middle"].isna().sum() == 19

    def test_band_ordering(self):
        df = _make_price_df(list(range(100, 130)))
        result = compute_bollinger_bands(df)
        valid = result.dropna()
        assert (valid["bb_upper"] >= valid["bb_middle"]).all()
        assert (valid["bb_middle"] >= valid["bb_lower"]).all()

    def test_custom_window(self):
        df = _make_price_df(list(range(100, 115)))
        result = compute_bollinger_bands(df, window=5)
        assert result["bb_middle"].isna().sum() == 4

    def test_length_matches_input(self):
        df = _make_price_df(list(range(100, 130)))
        result = compute_bollinger_bands(df)
        assert len(result) == len(df)


# ===================================================================
# compute_macd
# ===================================================================


@pytest.mark.unit
class TestComputeMacd:
    def test_output_columns(self):
        df = _make_price_df(list(range(100, 140)))
        result = compute_macd(df)
        assert list(result.columns) == ["date", "macd_line", "signal_line", "histogram"]

    def test_histogram_approximates_diff(self):
        df = _make_price_df(list(range(100, 140)))
        result = compute_macd(df)
        diff = result["macd_line"] - result["signal_line"]
        # Allow small rounding tolerance since each column is independently rounded
        assert np.allclose(diff, result["histogram"], atol=1e-3)

    def test_length_matches_input(self):
        df = _make_price_df(list(range(100, 140)))
        result = compute_macd(df)
        assert len(result) == len(df)

    def test_no_nan(self):
        # EWM doesn't produce NaN for the first values (unlike rolling)
        df = _make_price_df(list(range(100, 140)))
        result = compute_macd(df)
        assert result["macd_line"].isna().sum() == 0


# ===================================================================
# resample_ohlcv
# ===================================================================


@pytest.mark.unit
class TestResampleOhlcv:
    def _make_daily(self, n=30):
        dates = pd.bdate_range("2025-01-01", periods=n)
        return pd.DataFrame(
            {
                "date": dates.strftime("%Y-%m-%d"),
                "open": [100 + i for i in range(n)],
                "high": [105 + i for i in range(n)],
                "low": [95 + i for i in range(n)],
                "close": [102 + i for i in range(n)],
                "volume": [1_000_000] * n,
            }
        )

    def test_weekly_fewer_rows(self):
        df = self._make_daily(30)
        result = resample_ohlcv(df, "W")
        assert len(result) < len(df)

    def test_weekly_aggregation_rules(self):
        # Start on a Monday so 5 bdays = exactly one week
        dates = pd.bdate_range("2025-01-06", periods=5)  # Mon Jan 6
        df = pd.DataFrame(
            {
                "date": dates.strftime("%Y-%m-%d"),
                "open": [100, 101, 102, 103, 104],
                "high": [105, 106, 107, 108, 109],
                "low": [95, 96, 97, 98, 99],
                "close": [102, 103, 104, 105, 106],
                "volume": [1_000_000] * 5,
            }
        )
        result = resample_ohlcv(df, "W")
        assert len(result) == 1
        # open = first day's open
        assert result["open"].iloc[0] == 100
        # high = max of all days' highs
        assert result["high"].iloc[0] == 109
        # low = min of all days' lows
        assert result["low"].iloc[0] == 95
        # close = last day's close
        assert result["close"].iloc[0] == 106
        # volume = sum
        assert result["volume"].iloc[0] == 5_000_000

    def test_monthly_resampling(self):
        df = self._make_daily(60)
        result = resample_ohlcv(df, "ME")
        assert len(result) < len(df)

    def test_output_columns(self):
        df = self._make_daily(30)
        result = resample_ohlcv(df, "W")
        assert "date" in result.columns
        assert "open" in result.columns
        assert "close" in result.columns


# ===================================================================
# Edge cases — Bollinger Bands
# ===================================================================


@pytest.mark.unit
class TestBollingerBandsEdgeCases:
    def test_insufficient_data_all_nan(self):
        """Fewer rows than window → all NaN."""
        df = _make_price_df([100, 101, 102])
        result = compute_bollinger_bands(df, window=20)
        assert result["bb_middle"].isna().all()

    def test_constant_prices_zero_bandwidth(self):
        """Constant prices → upper == middle == lower (std=0)."""
        df = _make_price_df([100.0] * 25)
        result = compute_bollinger_bands(df, window=20)
        valid = result.dropna()
        assert (valid["bb_upper"] == valid["bb_middle"]).all()
        assert (valid["bb_lower"] == valid["bb_middle"]).all()

    def test_single_row(self):
        """Single row → all NaN, no crash."""
        df = _make_price_df([100])
        result = compute_bollinger_bands(df, window=20)
        assert len(result) == 1
        assert result["bb_middle"].isna().all()


# ===================================================================
# Edge cases — MACD
# ===================================================================


@pytest.mark.unit
class TestMacdEdgeCases:
    def test_constant_prices_zero_macd(self):
        """Constant prices → MACD line, signal, histogram all zero."""
        df = _make_price_df([100.0] * 30)
        result = compute_macd(df)
        assert (result["macd_line"] == 0).all()
        assert (result["signal_line"] == 0).all()
        assert (result["histogram"] == 0).all()

    def test_single_row(self):
        """Single row → MACD computable (EWM handles it), no crash."""
        df = _make_price_df([100])
        result = compute_macd(df)
        assert len(result) == 1
        assert not result["macd_line"].isna().any()

    def test_two_rows(self):
        """Two rows → no crash, values computed."""
        df = _make_price_df([100, 110])
        result = compute_macd(df)
        assert len(result) == 2


# ===================================================================
# Edge cases — resample_ohlcv
# ===================================================================


@pytest.mark.unit
class TestResampleEdgeCases:
    def test_single_row_weekly(self):
        """Single row resampled to weekly → 1 row."""
        df = pd.DataFrame(
            {
                "date": ["2025-01-06"],
                "open": [100],
                "high": [105],
                "low": [95],
                "close": [102],
                "volume": [1_000_000],
            }
        )
        result = resample_ohlcv(df, "W")
        assert len(result) == 1

    def test_empty_df(self):
        """Empty DataFrame → empty result, no crash."""
        df = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        result = resample_ohlcv(df, "W")
        assert result.empty
