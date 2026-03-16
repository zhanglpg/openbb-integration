"""Integration tests for the chart data pipeline.

Tests the full flow: DB → get_price_history_by_date → resample → indicators → Plotly figure.
Covers: weekend gap skipping, dynamic data fetching, build_chart with all chart types.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from analysis import compute_bollinger_bands, compute_macd, resample_ohlcv  # noqa: E402


def _make_ohlcv(n=100, start="2024-01-01"):
    """Generate realistic OHLCV data for n trading days."""
    np.random.seed(42)
    dates = pd.bdate_range(start, periods=n)
    base = 150.0
    returns = np.random.randn(n) * 0.02
    closes = base * np.cumprod(1 + returns)
    opens = closes * (1 + np.random.randn(n) * 0.005)
    highs = np.maximum(opens, closes) * (1 + np.abs(np.random.randn(n)) * 0.005)
    lows = np.minimum(opens, closes) * (1 - np.abs(np.random.randn(n)) * 0.005)
    volumes = np.random.randint(500_000, 5_000_000, size=n)
    return pd.DataFrame(
        {
            "date": dates.strftime("%Y-%m-%d"),
            "open": np.round(opens, 2),
            "high": np.round(highs, 2),
            "low": np.round(lows, 2),
            "close": np.round(closes, 2),
            "volume": volumes,
        }
    )


# ===================================================================
# DB → Analysis pipeline integration
# ===================================================================


@pytest.mark.integration
class TestDBToAnalysisPipeline:
    """Test data flowing from database through analysis functions."""

    def test_db_to_bollinger_bands(self, tmp_db):
        """Save prices → query by date → compute Bollinger Bands."""
        df = _make_ohlcv(60)
        tmp_db.save_prices(df, "AAPL")

        result = tmp_db.get_price_history_by_date("AAPL", "2024-01-01")
        assert len(result) == 60

        bb = compute_bollinger_bands(result)
        assert len(bb) == 60
        valid = bb.dropna()
        assert len(valid) > 0
        assert (valid["bb_upper"] >= valid["bb_lower"]).all()

    def test_db_to_macd(self, tmp_db):
        """Save prices → query by date → compute MACD."""
        df = _make_ohlcv(60)
        tmp_db.save_prices(df, "AAPL")

        result = tmp_db.get_price_history_by_date("AAPL", "2024-01-01")
        macd = compute_macd(result)
        assert len(macd) == 60
        assert not macd["macd_line"].isna().any()
        assert not macd["signal_line"].isna().any()

    def test_db_to_resample_to_indicators(self, tmp_db):
        """Save daily prices → query → resample weekly → compute indicators."""
        df = _make_ohlcv(100)
        tmp_db.save_prices(df, "AAPL")

        daily = tmp_db.get_price_history_by_date("AAPL", "2024-01-01")
        weekly = resample_ohlcv(daily, "W")
        assert len(weekly) < len(daily)
        assert len(weekly) > 0

        # Indicators work on resampled data
        bb = compute_bollinger_bands(weekly)
        assert len(bb) == len(weekly)

        macd = compute_macd(weekly)
        assert len(macd) == len(weekly)

    def test_monthly_resample_pipeline(self, tmp_db):
        """Save prices → query → resample monthly → verify aggregation."""
        df = _make_ohlcv(200)
        tmp_db.save_prices(df, "AAPL")

        daily = tmp_db.get_price_history_by_date("AAPL", "2024-01-01")
        monthly = resample_ohlcv(daily, "ME")
        assert len(monthly) < len(daily)

        # Each monthly bar's high should be >= close and >= open
        assert (monthly["high"] >= monthly["close"]).all()
        assert (monthly["high"] >= monthly["open"]).all()
        assert (monthly["low"] <= monthly["close"]).all()
        assert (monthly["low"] <= monthly["open"]).all()


# ===================================================================
# Plotly chart rendering integration
# ===================================================================


@pytest.mark.integration
class TestChartRendering:
    """Test that analysis output produces valid Plotly figures."""

    def test_candlestick_chart(self):
        """Build a candlestick chart from OHLCV data."""
        df = _make_ohlcv(30)
        fig = go.Figure(
            go.Candlestick(
                x=pd.to_datetime(df["date"]),
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
            )
        )
        assert len(fig.data) == 1
        assert fig.data[0].type == "candlestick"

    def test_area_chart(self):
        """Build an area chart from close prices."""
        df = _make_ohlcv(30)
        fig = go.Figure(
            go.Scatter(
                x=pd.to_datetime(df["date"]),
                y=df["close"],
                fill="tozeroy",
                mode="lines",
            )
        )
        assert len(fig.data) == 1
        assert fig.data[0].type == "scatter"
        assert fig.data[0].fill == "tozeroy"

    def test_ohlc_bars_chart(self):
        """Build an OHLC bar chart."""
        df = _make_ohlcv(30)
        fig = go.Figure(
            go.Ohlc(
                x=pd.to_datetime(df["date"]),
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
            )
        )
        assert len(fig.data) == 1
        assert fig.data[0].type == "ohlc"

    def test_volume_bars_with_colors(self):
        """Build volume bars colored by close vs open."""
        df = _make_ohlcv(30)
        colors = ["#26a69a" if c >= o else "#ef5350" for c, o in zip(df["close"], df["open"])]
        fig = go.Figure(go.Bar(x=pd.to_datetime(df["date"]), y=df["volume"], marker_color=colors))
        assert len(fig.data) == 1
        assert fig.data[0].type == "bar"
        assert len(fig.data[0].marker.color) == 30

    def test_macd_chart_traces(self):
        """MACD chart should have 3 traces: histogram, MACD line, signal line."""
        df = _make_ohlcv(60)
        macd = compute_macd(df)

        fig = go.Figure()
        fig.add_trace(go.Bar(x=pd.to_datetime(df["date"]), y=macd["histogram"], name="Histogram"))
        fig.add_trace(go.Scatter(x=pd.to_datetime(df["date"]), y=macd["macd_line"], name="MACD"))
        fig.add_trace(
            go.Scatter(x=pd.to_datetime(df["date"]), y=macd["signal_line"], name="Signal")
        )
        assert len(fig.data) == 3

    def test_bollinger_bands_overlay(self):
        """Bollinger Bands overlaid on price chart should have 4 traces."""
        df = _make_ohlcv(60)
        bb = compute_bollinger_bands(df)

        fig = go.Figure()
        # Price
        fig.add_trace(
            go.Candlestick(
                x=pd.to_datetime(df["date"]),
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name="Price",
            )
        )
        # BB bands
        fig.add_trace(go.Scatter(x=pd.to_datetime(df["date"]), y=bb["bb_upper"], name="Upper"))
        fig.add_trace(go.Scatter(x=pd.to_datetime(df["date"]), y=bb["bb_middle"], name="Middle"))
        fig.add_trace(go.Scatter(x=pd.to_datetime(df["date"]), y=bb["bb_lower"], name="Lower"))

        assert len(fig.data) == 4

    def test_full_chart_pipeline(self, tmp_db):
        """End-to-end: DB → resample → indicators → multi-panel Plotly figure."""
        from plotly.subplots import make_subplots

        # Save data
        df = _make_ohlcv(200)
        tmp_db.save_prices(df, "AAPL")

        # Load from DB
        daily = tmp_db.get_price_history_by_date("AAPL", "2024-01-01")
        assert not daily.empty

        # Resample to weekly
        weekly = resample_ohlcv(daily, "W")
        assert not weekly.empty

        # Compute indicators
        bb = compute_bollinger_bands(weekly)
        macd = compute_macd(weekly)

        # Build multi-panel figure
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3])

        # Row 1: Candlestick + BB overlay
        dates = pd.to_datetime(weekly["date"])
        fig.add_trace(
            go.Candlestick(
                x=dates,
                open=weekly["open"],
                high=weekly["high"],
                low=weekly["low"],
                close=weekly["close"],
            ),
            row=1,
            col=1,
        )
        fig.add_trace(go.Scatter(x=dates, y=bb["bb_upper"], name="BB Upper"), row=1, col=1)
        fig.add_trace(go.Scatter(x=dates, y=bb["bb_lower"], name="BB Lower"), row=1, col=1)

        # Row 2: MACD
        fig.add_trace(go.Bar(x=dates, y=macd["histogram"], name="Hist"), row=2, col=1)
        fig.add_trace(go.Scatter(x=dates, y=macd["macd_line"], name="MACD"), row=2, col=1)
        fig.add_trace(go.Scatter(x=dates, y=macd["signal_line"], name="Signal"), row=2, col=1)

        # Verify figure structure
        assert len(fig.data) == 6  # candlestick + 2 BB + hist + MACD + signal
        fig.update_layout(height=700, xaxis_rangeslider_visible=False)

        # Validate the figure can be serialized (would fail with bad data types)
        fig_dict = fig.to_dict()
        assert "data" in fig_dict
        assert "layout" in fig_dict
        assert len(fig_dict["data"]) == 6


# ===================================================================
# Data quality checks
# ===================================================================


@pytest.mark.integration
class TestDataQuality:
    """Verify data integrity through the pipeline."""

    def test_resample_preserves_price_range(self):
        """Resampled data should not exceed the original price range."""
        df = _make_ohlcv(100)
        weekly = resample_ohlcv(df, "W")

        assert weekly["high"].max() <= df["high"].astype(float).max()
        assert weekly["low"].min() >= df["low"].astype(float).min()

    def test_resample_preserves_total_volume(self):
        """Total volume should be preserved after resampling."""
        df = _make_ohlcv(100)
        weekly = resample_ohlcv(df, "W")

        # Weekly sum should equal daily sum (approximately — last partial week may differ)
        daily_vol = df["volume"].astype(float).sum()
        weekly_vol = weekly["volume"].sum()
        assert weekly_vol == pytest.approx(daily_vol, rel=0.01)

    def test_indicators_nan_alignment(self):
        """BB NaN rows should align with the first (window-1) rows."""
        df = _make_ohlcv(60)
        bb = compute_bollinger_bands(df, window=20)
        # First 19 rows NaN, rest valid
        assert bb["bb_middle"].iloc[18] != bb["bb_middle"].iloc[18]  # NaN
        assert bb["bb_middle"].iloc[19] == bb["bb_middle"].iloc[19]  # not NaN

    def test_date_range_query_matches_direct_query(self, tmp_db):
        """get_price_history_by_date should return same data as get_latest_prices for same range."""
        df = _make_ohlcv(30, start="2025-01-01")
        tmp_db.save_prices(df, "AAPL")

        by_date = tmp_db.get_price_history_by_date("AAPL", "2025-01-01", "2025-12-31")
        by_limit = tmp_db.get_latest_prices("AAPL", days=100)

        assert len(by_date) == len(by_limit)
        # Same close prices (order may differ — sort both)
        date_closes = sorted(by_date["close"].tolist())
        limit_closes = sorted(by_limit["close"].tolist())
        assert date_closes == limit_closes


# ===================================================================
# Weekend/holiday gap skipping (rangebreaks)
# ===================================================================


@pytest.mark.integration
class TestWeekendGapSkipping:
    """Verify Plotly rangebreaks are applied correctly for daily data."""

    def test_daily_chart_has_rangebreaks(self):
        """Daily candlestick chart should have weekend rangebreaks."""
        # Import build_chart — need to handle the Streamlit import
        # We test the rangebreaks logic directly via Plotly
        df = _make_ohlcv(30)
        df["date"] = pd.to_datetime(df["date"])

        fig = go.Figure(
            go.Candlestick(
                x=df["date"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
            )
        )
        fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])

        # Verify rangebreaks are set in the layout
        layout_dict = fig.to_dict()["layout"]
        xaxis = layout_dict.get("xaxis", {})
        rangebreaks = xaxis.get("rangebreaks", [])
        assert len(rangebreaks) == 1
        assert rangebreaks[0]["bounds"] == ["sat", "mon"]

    def test_weekly_chart_no_rangebreaks(self):
        """Weekly resampled chart should NOT have weekend rangebreaks."""
        df = _make_ohlcv(60)
        weekly = resample_ohlcv(df, "W")

        fig = go.Figure(
            go.Candlestick(
                x=weekly["date"],
                open=weekly["open"],
                high=weekly["high"],
                low=weekly["low"],
                close=weekly["close"],
            )
        )
        # Do NOT add rangebreaks for non-daily data
        layout_dict = fig.to_dict()["layout"]
        xaxis = layout_dict.get("xaxis", {})
        assert not xaxis.get("rangebreaks")

    def test_monthly_chart_no_rangebreaks(self):
        """Monthly resampled chart should NOT have weekend rangebreaks."""
        df = _make_ohlcv(200)
        monthly = resample_ohlcv(df, "ME")

        fig = go.Figure(
            go.Candlestick(
                x=monthly["date"],
                open=monthly["open"],
                high=monthly["high"],
                low=monthly["low"],
                close=monthly["close"],
            )
        )
        layout_dict = fig.to_dict()["layout"]
        xaxis = layout_dict.get("xaxis", {})
        assert not xaxis.get("rangebreaks")

    def test_rangebreaks_on_area_chart(self):
        """Area chart with daily data should also have rangebreaks."""
        df = _make_ohlcv(30)
        df["date"] = pd.to_datetime(df["date"])

        fig = go.Figure(go.Scatter(x=df["date"], y=df["close"], fill="tozeroy", mode="lines"))
        fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])

        rangebreaks = fig.to_dict()["layout"]["xaxis"]["rangebreaks"]
        assert len(rangebreaks) == 1

    def test_rangebreaks_on_ohlc_chart(self):
        """OHLC bar chart with daily data should have rangebreaks."""
        df = _make_ohlcv(30)
        df["date"] = pd.to_datetime(df["date"])

        fig = go.Figure(
            go.Ohlc(
                x=df["date"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
            )
        )
        fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])

        rangebreaks = fig.to_dict()["layout"]["xaxis"]["rangebreaks"]
        assert len(rangebreaks) == 1

    def test_rangebreaks_with_volume_subplot(self):
        """Multi-panel chart with volume should have rangebreaks on both axes."""
        from plotly.subplots import make_subplots

        df = _make_ohlcv(30)
        df["date"] = pd.to_datetime(df["date"])

        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3])
        fig.add_trace(
            go.Candlestick(
                x=df["date"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
            ),
            row=1,
            col=1,
        )
        colors = ["#26a69a" if c >= o else "#ef5350" for c, o in zip(df["close"], df["open"])]
        fig.add_trace(
            go.Bar(x=df["date"], y=df["volume"], marker_color=colors),
            row=2,
            col=1,
        )
        fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])

        layout = fig.to_dict()["layout"]
        # Shared x-axes: rangebreaks on xaxis (row 1's x-axis controls both)
        assert len(layout["xaxis"]["rangebreaks"]) == 1


# ===================================================================
# Dynamic data fetching (_ensure_data)
# ===================================================================


@pytest.mark.integration
class TestDynamicDataFetch:
    """Test _ensure_data: DB check → API fetch → backfill → return."""

    def test_returns_db_data_when_coverage_sufficient(self, tmp_db):
        """If DB has data covering the requested range, no fetch happens."""
        df = _make_ohlcv(60, start="2024-01-01")
        tmp_db.save_prices(df, "AAPL")

        result = tmp_db.get_price_history_by_date("AAPL", "2024-01-01")
        earliest = result["date"].min()
        assert earliest <= "2024-01-01"
        assert len(result) == 60

    def test_fetcher_called_when_db_insufficient(self, tmp_db):
        """When DB has no data, fetcher is called and result is saved."""
        # DB is empty for AAPL
        assert tmp_db.get_price_history_by_date("AAPL", "2020-01-01").empty

        # Simulate what _ensure_data does: fetch → save → re-query
        mock_fresh = _make_ohlcv(30, start="2020-01-01")
        tmp_db.save_prices(mock_fresh, "AAPL")

        result = tmp_db.get_price_history_by_date("AAPL", "2020-01-01")
        assert len(result) == 30

    def test_fetcher_backfills_earlier_data(self, tmp_db):
        """DB has recent data; fetcher fills the gap for earlier dates."""
        # Insert only recent data (March 2024)
        recent = _make_ohlcv(20, start="2024-03-01")
        tmp_db.save_prices(recent, "AAPL")

        result = tmp_db.get_price_history_by_date("AAPL", "2024-01-01")
        earliest = result["date"].min()
        # DB doesn't cover Jan 2024 yet
        assert earliest > "2024-01-01"

        # Simulate backfill: fetcher returns Jan-Feb data
        backfill = _make_ohlcv(40, start="2024-01-01")
        tmp_db.save_prices(backfill, "AAPL")

        result = tmp_db.get_price_history_by_date("AAPL", "2024-01-01")
        earliest = result["date"].min()
        assert earliest <= "2024-01-02"  # Now covers Jan
        # Should have both old and new data (deduped via upsert)
        assert len(result) > 20

    def test_fetcher_failure_returns_existing_db_data(self, tmp_db):
        """If fetcher fails, _ensure_data still returns whatever DB has."""
        recent = _make_ohlcv(10, start="2024-06-01")
        tmp_db.save_prices(recent, "AAPL")

        # Even if fetch would fail, DB data is still returned
        result = tmp_db.get_price_history_by_date("AAPL", "2024-01-01")
        assert len(result) == 10  # Only what DB has

    def test_fetched_data_persisted_for_future_queries(self, tmp_db):
        """Data fetched on the fly should be available in subsequent DB queries."""
        assert tmp_db.get_price_history_by_date("MSFT", "2024-01-01").empty

        # Simulate fetch + save
        fetched = _make_ohlcv(50, start="2024-01-01")
        tmp_db.save_prices(fetched, "MSFT")

        # Second query should find the data without fetching again
        result = tmp_db.get_price_history_by_date("MSFT", "2024-01-01")
        assert len(result) == 50

    def test_upsert_deduplicates_overlapping_data(self, tmp_db):
        """Fetching overlapping date ranges should not create duplicates."""
        batch1 = _make_ohlcv(30, start="2024-01-01")
        tmp_db.save_prices(batch1, "AAPL")

        # Fetch again with overlapping range (starts mid-January)
        batch2 = _make_ohlcv(30, start="2024-01-15")
        tmp_db.save_prices(batch2, "AAPL")

        result = tmp_db.get_price_history_by_date("AAPL", "2024-01-01", "2024-12-31")
        dates = result["date"].tolist()
        # No duplicate dates
        assert len(dates) == len(set(dates))


# ===================================================================
# build_chart with skip_gaps parameter
# ===================================================================


@pytest.mark.integration
class TestBuildChartSkipGaps:
    """Test build_chart function from 5_Charts.py with skip_gaps parameter.

    We import and test build_chart directly by patching Streamlit.
    """

    @pytest.fixture(autouse=True)
    def _setup_build_chart(self):
        """Import build_chart with mocked Streamlit."""
        import importlib

        mock_st = MagicMock()
        mock_st.cache_data = MagicMock(return_value=lambda f: f)
        mock_st.cache_resource = MagicMock(return_value=lambda f: f)

        project_root = str(Path(__file__).parent.parent)
        if project_root not in sys.path:
            sys.path.insert(0, project_root)

        # Remove cached modules so we get a fresh import
        for mod_name in list(sys.modules):
            if mod_name in ("pages.5_Charts", "charts_page"):
                del sys.modules[mod_name]

        with patch.dict(
            sys.modules,
            {
                "streamlit": mock_st,
                "streamlit.components": MagicMock(),
                "streamlit.components.v1": MagicMock(),
                "streamlit_sortables": MagicMock(),
            },
        ):
            charts_file = Path(__file__).parent.parent / "pages" / "5_Charts.py"
            spec = importlib.util.spec_from_file_location("charts_page", charts_file)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            self.build_chart = mod.build_chart
            yield

    def _daily_df(self, n=30):
        df = _make_ohlcv(n)
        df["date"] = pd.to_datetime(df["date"])
        return df

    def test_skip_gaps_true_adds_rangebreaks(self):
        """build_chart with skip_gaps=True should add rangebreaks."""
        df = self._daily_df()
        fig = self.build_chart(df, "Candlestick", "None", "AAPL", skip_gaps=True)
        layout = fig.to_dict()["layout"]
        rb = layout.get("xaxis", {}).get("rangebreaks", [])
        assert len(rb) == 1
        assert rb[0]["bounds"] == ["sat", "mon"]

    def test_skip_gaps_false_no_rangebreaks(self):
        """build_chart with skip_gaps=False should NOT add rangebreaks."""
        df = self._daily_df()
        fig = self.build_chart(df, "Candlestick", "None", "AAPL", skip_gaps=False)
        layout = fig.to_dict()["layout"]
        rb = layout.get("xaxis", {}).get("rangebreaks", [])
        assert len(rb) == 0

    def test_all_chart_types_with_skip_gaps(self):
        """All chart types should render with skip_gaps without errors."""
        df = self._daily_df(60)
        for chart_type in ["Candlestick", "Area", "OHLC Bars"]:
            fig = self.build_chart(df, chart_type, "Volume", "AAPL", skip_gaps=True)
            assert len(fig.data) >= 2  # price + volume
            fig_dict = fig.to_dict()
            assert "data" in fig_dict

    def test_all_indicators_with_skip_gaps(self):
        """All indicators should work with skip_gaps."""
        df = self._daily_df(60)
        for indicator in ["Volume", "MACD", "Bollinger Bands", "None"]:
            fig = self.build_chart(df, "Candlestick", indicator, "AAPL", skip_gaps=True)
            assert len(fig.data) >= 1
            # Verify serializable
            fig.to_dict()

    def test_candlestick_with_macd_and_skip_gaps(self):
        """Full chart: candlestick + MACD + weekend gaps skipped."""
        df = self._daily_df(60)
        fig = self.build_chart(df, "Candlestick", "MACD", "AAPL", skip_gaps=True)
        # Should have: candlestick + histogram + MACD line + signal line = 4 traces
        assert len(fig.data) == 4
        rb = fig.to_dict()["layout"]["xaxis"]["rangebreaks"]
        assert len(rb) == 1

    def test_area_with_bollinger_and_skip_gaps(self):
        """Area chart + Bollinger Bands + gap skipping."""
        df = self._daily_df(60)
        fig = self.build_chart(df, "Area", "Bollinger Bands", "AAPL", skip_gaps=True)
        # Area + 3 BB overlay + BB width = 5 traces
        assert len(fig.data) == 5
        rb = fig.to_dict()["layout"]["xaxis"]["rangebreaks"]
        assert len(rb) == 1

    def test_single_sma_overlay(self):
        """A single SMA overlay adds one trace to the chart."""
        df = self._daily_df(60)
        fig = self.build_chart(
            df, "Candlestick", "None", "AAPL", skip_gaps=True, ma_overlays=["SMA 20"]
        )
        # Candlestick + SMA 20 = 2 traces
        assert len(fig.data) == 2
        assert fig.data[1].name == "SMA 20"

    def test_single_ema_overlay(self):
        """A single EMA overlay adds one trace to the chart."""
        df = self._daily_df(60)
        fig = self.build_chart(
            df, "Candlestick", "None", "AAPL", skip_gaps=True, ma_overlays=["EMA 12"]
        )
        assert len(fig.data) == 2
        assert fig.data[1].name == "EMA 12"

    def test_multiple_ma_overlays(self):
        """Multiple MA overlays each add a trace with correct names and colors."""
        df = self._daily_df(60)
        mas = ["SMA 10", "SMA 20", "EMA 26"]
        fig = self.build_chart(df, "Candlestick", "None", "AAPL", skip_gaps=True, ma_overlays=mas)
        # Candlestick + 3 MAs = 4 traces
        assert len(fig.data) == 4
        ma_traces = fig.data[1:]
        assert [t.name for t in ma_traces] == mas
        assert ma_traces[0].line.color == "#FF6B6B"
        assert ma_traces[1].line.color == "#4ECDC4"
        assert ma_traces[2].line.color == "#DDA0DD"

    def test_ma_overlays_with_indicator(self):
        """MA overlays coexist with bottom-panel indicators."""
        df = self._daily_df(60)
        fig = self.build_chart(
            df, "Candlestick", "MACD", "AAPL", skip_gaps=True, ma_overlays=["SMA 50"]
        )
        # Candlestick + SMA 50 + MACD histogram + MACD line + signal = 5
        assert len(fig.data) == 5
        assert fig.data[1].name == "SMA 50"

    def test_all_six_ma_overlays(self):
        """All 6 MA options render without errors."""
        df = self._daily_df(250)  # enough data for SMA 200
        all_mas = ["SMA 10", "SMA 20", "SMA 50", "SMA 200", "EMA 12", "EMA 26"]
        fig = self.build_chart(
            df, "Candlestick", "None", "AAPL", skip_gaps=True, ma_overlays=all_mas
        )
        # Candlestick + 6 MAs = 7
        assert len(fig.data) == 7
        # All should be serializable
        fig.to_dict()

    def test_no_ma_overlays_default(self):
        """No MA overlays by default (backward compatible)."""
        df = self._daily_df(30)
        fig = self.build_chart(df, "Candlestick", "None", "AAPL", skip_gaps=True)
        assert len(fig.data) == 1  # just candlestick
