#!/usr/bin/env python3
"""Interactive stock charts with technical indicators."""

import logging  # noqa: I001
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from shared import get_db, render_sidebar_controls  # must be first: adds src/ to sys.path

from analysis import compute_bollinger_bands, compute_macd, resample_ohlcv
from config import WATCHLIST
from database import Database
from fetcher import DataFetcher

logger = logging.getLogger(__name__)

# Page config
st.set_page_config(page_title="Charts", page_icon="📈", layout="wide")

ALL_SYMBOLS = sorted(set(sym for symbols in WATCHLIST.values() for sym in symbols))

# Time range definitions: (days_back, auto_granularity)
TIME_RANGES = {
    "1d": (1, "D"),
    "7d": (7, "D"),
    "1m": (30, "D"),
    "1y": (365, "D"),
    "3y": (1095, "W"),
    "10y": (3650, "ME"),
}


def _ensure_data(db: Database, symbol: str, start_date: str) -> pd.DataFrame:
    """Load data from DB; if insufficient, fetch from OpenBB and backfill.

    Returns:
        OHLCV DataFrame sorted by date ascending.
    """
    df = db.get_price_history_by_date(symbol, start_date)

    if not df.empty:
        earliest_in_db = df["date"].min()
        # If DB covers the requested start, return as-is
        if earliest_in_db <= start_date:
            return df

    # DB doesn't have enough data — fetch from OpenBB on the fly
    try:
        fetcher = DataFetcher()
        end_date = datetime.now().strftime("%Y-%m-%d")
        fresh = fetcher.fetch_historical_prices(symbol, start_date=start_date, end_date=end_date)
        if fresh is not None and not fresh.empty:
            # Persist to DB for future use
            db.save_prices(fresh, symbol)
            # Re-query to get unified, deduped data
            df = db.get_price_history_by_date(symbol, start_date)
    except Exception as e:
        logger.warning("On-the-fly fetch failed for %s: %s", symbol, e)

    return df


@st.cache_data(ttl=3600, show_spinner="Loading chart data...")
def load_chart_data(_db, symbol: str, days_back: int) -> pd.DataFrame:
    """Load OHLCV data for a symbol, fetching from API if needed."""
    start = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    df = _ensure_data(_db, symbol, start)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


def build_chart(df, chart_type, indicator, symbol, skip_gaps):
    """Build a Plotly figure with main price chart and bottom indicator panel."""
    has_indicator = indicator != "None"
    row_heights = [0.7, 0.3] if has_indicator else [1.0]
    rows = 2 if has_indicator else 1

    fig = make_subplots(
        rows=rows,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=row_heights,
    )

    # --- Row 1: Main price chart ---
    if chart_type == "Candlestick":
        fig.add_trace(
            go.Candlestick(
                x=df["date"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name="Price",
                increasing_line_color="#26a69a",
                decreasing_line_color="#ef5350",
            ),
            row=1,
            col=1,
        )
    elif chart_type == "Area":
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["close"],
                fill="tozeroy",
                mode="lines",
                name="Close",
                line=dict(color="#2196F3", width=1.5),
                fillcolor="rgba(33, 150, 243, 0.15)",
            ),
            row=1,
            col=1,
        )
    elif chart_type == "OHLC Bars":
        fig.add_trace(
            go.Ohlc(
                x=df["date"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name="Price",
                increasing_line_color="#26a69a",
                decreasing_line_color="#ef5350",
            ),
            row=1,
            col=1,
        )

    # --- Bollinger Bands overlay on Row 1 ---
    if indicator == "Bollinger Bands":
        bb = compute_bollinger_bands(df).dropna()
        fig.add_trace(
            go.Scatter(
                x=bb["date"],
                y=bb["bb_upper"].tolist(),
                mode="lines",
                name="BB Upper",
                line=dict(color="#FF9800", width=1.5, dash="dot"),
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=bb["date"],
                y=bb["bb_middle"].tolist(),
                mode="lines",
                name="BB Middle",
                line=dict(color="#FF9800", width=1.5),
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=bb["date"],
                y=bb["bb_lower"].tolist(),
                mode="lines",
                name="BB Lower",
                line=dict(color="#FF9800", width=1.5, dash="dot"),
                fill="tonexty",
                fillcolor="rgba(255, 152, 0, 0.1)",
            ),
            row=1,
            col=1,
        )

    # --- Row 2: Bottom indicator ---
    if indicator == "Volume" and "volume" in df.columns:
        colors = ["#26a69a" if c >= o else "#ef5350" for c, o in zip(df["close"], df["open"])]
        fig.add_trace(
            go.Bar(
                x=df["date"],
                y=df["volume"],
                name="Volume",
                marker_color=colors,
                opacity=0.7,
            ),
            row=2,
            col=1,
        )
        fig.update_yaxes(title_text="Volume", row=2, col=1)

    elif indicator == "MACD":
        macd_df = compute_macd(df)
        hist_colors = ["#26a69a" if v >= 0 else "#ef5350" for v in macd_df["histogram"]]
        fig.add_trace(
            go.Bar(
                x=df["date"],
                y=macd_df["histogram"],
                name="Histogram",
                marker_color=hist_colors,
                opacity=0.7,
            ),
            row=2,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=macd_df["macd_line"],
                mode="lines",
                name="MACD",
                line=dict(color="#2196F3", width=1.5),
            ),
            row=2,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=macd_df["signal_line"],
                mode="lines",
                name="Signal",
                line=dict(color="#FF9800", width=1.5),
            ),
            row=2,
            col=1,
        )
        fig.update_yaxes(title_text="MACD", row=2, col=1)

    elif indicator == "Bollinger Bands":
        # Show bandwidth in Row 2
        bb = compute_bollinger_bands(df).dropna()
        bandwidth = (bb["bb_upper"] - bb["bb_lower"]).tolist()
        fig.add_trace(
            go.Scatter(
                x=bb["date"],
                y=bandwidth,
                mode="lines",
                name="BB Width",
                fill="tozeroy",
                line=dict(color="#FF9800", width=1),
                fillcolor="rgba(255, 152, 0, 0.15)",
            ),
            row=2,
            col=1,
        )
        fig.update_yaxes(title_text="BB Width", row=2, col=1)

    # --- Layout ---
    fig.update_layout(
        title=f"{symbol}",
        height=700,
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=60, r=20, t=60, b=40),
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(128, 128, 128, 0.2)")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(128, 128, 128, 0.2)")
    fig.update_yaxes(title_text="Price", row=1, col=1)

    # Skip weekends/holidays for daily data
    if skip_gaps:
        fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])

    return fig


def main():
    st.title("📈 Charts")

    render_sidebar_controls()

    # --- Sidebar controls ---
    # Default to the symbol selected on the dashboard (via session_state)
    default_sym = st.session_state.get("selected_symbol", ALL_SYMBOLS[0])
    default_idx = ALL_SYMBOLS.index(default_sym) if default_sym in ALL_SYMBOLS else 0
    symbol = st.sidebar.selectbox("Symbol", ALL_SYMBOLS, index=default_idx)
    chart_type = st.sidebar.radio("Chart Type", ["Candlestick", "Area", "OHLC Bars"])
    time_range = st.sidebar.radio("Time Range", list(TIME_RANGES.keys()), index=2, horizontal=True)

    granularity = st.sidebar.selectbox("Granularity", ["Auto", "Daily", "Weekly", "Monthly"])
    indicator = st.sidebar.selectbox(
        "Bottom Indicator", ["Volume", "MACD", "Bollinger Bands", "None"]
    )

    # --- Resolve granularity ---
    days_back, auto_freq = TIME_RANGES[time_range]
    if granularity == "Auto":
        freq = auto_freq
    elif granularity == "Weekly":
        freq = "W"
    elif granularity == "Monthly":
        freq = "ME"
    else:
        freq = "D"

    # --- Load data (with on-the-fly fetch if needed) ---
    db = get_db()
    df = load_chart_data(db, symbol, days_back)

    if df.empty:
        st.warning(f"No price data available for {symbol}.")
        return

    # --- Resample if needed ---
    if freq in ("W", "ME") and len(df) > 1:
        df = resample_ohlcv(df, freq)

    if df.empty:
        st.warning("Not enough data for this granularity.")
        return

    # Skip weekend/holiday gaps only for daily (non-resampled) data
    skip_gaps = freq == "D"

    # --- Build and render chart ---
    fig = build_chart(df, chart_type, indicator, symbol, skip_gaps)
    st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": True})

    # --- Metrics bar ---
    latest = df.iloc[-1]
    first = df.iloc[0]
    period_change = ((latest["close"] - first["close"]) / first["close"]) * 100

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Close", f"${latest['close']:.2f}")
    col2.metric("Change", f"{period_change:+.1f}%")
    col3.metric("High", f"${df['high'].max():.2f}")
    col4.metric("Low", f"${df['low'].min():.2f}")
    if "volume" in df.columns:
        col5.metric("Avg Volume", f"{df['volume'].mean():,.0f}")


if __name__ == "__main__":
    main()
