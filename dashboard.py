#!/usr/bin/env python3
"""
Streamlit Portfolio Dashboard
Real-time portfolio monitoring with OpenBB data pipeline
"""

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from config import WATCHLIST
from database import Database
from run_pipeline import run_full_pipeline

# Page config
st.set_page_config(
    page_title="Portfolio Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Derive portfolio structure and flat symbol list from config
PORTFOLIO = {category.title(): symbols for category, symbols in WATCHLIST.items()}
ALL_SYMBOLS = sorted(set(sym for symbols in WATCHLIST.values() for sym in symbols))


@st.cache_resource
def get_db():
    """Cached database connection (persists across Streamlit reruns)."""
    return Database()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_latest_prices(_db: Database) -> pd.DataFrame:
    """Get latest prices for all portfolio symbols (single batch query)."""
    return _db.get_latest_prices_batch(ALL_SYMBOLS)


@st.cache_data(ttl=300)
def get_latest_prices_with_change(_db: Database) -> pd.DataFrame:
    """Get latest prices with change percent for all symbols."""
    return _db.get_latest_prices_batch_with_previous(ALL_SYMBOLS)


@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_price_history(symbol: str, _db: Database, days: int = 90) -> pd.DataFrame:
    """Get price history for a symbol."""
    df = _db.get_latest_prices(symbol, days=days)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
    return df


@st.cache_data(ttl=3600)
def get_economic_indicators(_db: Database) -> pd.DataFrame:
    """Get latest economic indicators (delegates to Database method)."""
    key_indicators = ["VIXCLS", "DGS10", "T10Y2Y", "FEDFUNDS"]
    return _db.get_latest_economic_indicators(key_indicators)


@st.cache_data(ttl=3600)
def get_sec_filings(symbol: str, _db: Database, limit: int = 10) -> pd.DataFrame:
    """Get latest SEC filings for a symbol."""
    import glob

    sec_dir = Path(__file__).parent / "data" / "sec"
    pattern = str(sec_dir / f"{symbol}_sec_filings_*.parquet")
    files = sorted(glob.glob(pattern))

    if not files:
        return pd.DataFrame()

    df = pd.read_parquet(files[-1])
    # Filter for 10-K and 10-Q
    filings = df[df['report_type'].isin(['10-K', '10-Q', '8-K', '10-K/A', '10-Q/A'])]
    return filings.head(limit)


def refresh_data():
    """Run the data pipeline to fetch latest data."""
    with st.spinner("Fetching latest data..."):
        try:
            run_full_pipeline()
            st.success("✅ Data refreshed successfully!")
        except Exception as e:
            st.error(f"❌ Error refreshing data: {str(e)}")


def main():
    st.title("📊 Portfolio Dashboard")
    st.markdown("**Real-time portfolio monitoring** | OpenBB Data Pipeline")

    # Sidebar
    st.sidebar.header("Controls")

    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("🔄 Refresh Data", use_container_width=True):
            refresh_data()
            st.rerun()

    with col2:
        if st.button("🔃 Reset Cache", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    st.sidebar.divider()

    # Symbol selector
    selected_symbol = st.sidebar.selectbox(
        "Select Symbol",
        ALL_SYMBOLS,
        index=0
    )

    # Initialize database (cached)
    db = get_db()

    # =========================================================================
    # TOP SECTION: Portfolio Overview
    # =========================================================================
    st.subheader("Portfolio Overview")

    prices_with_change = get_latest_prices_with_change(db)

    if not prices_with_change.empty:
        # Build display data from batch result
        display_data = []
        for symbol in ALL_SYMBOLS:
            symbol_df = prices_with_change[prices_with_change['symbol'] == symbol]
            if len(symbol_df) >= 2:
                latest = symbol_df.iloc[0]
                previous = symbol_df.iloc[1]
                price = latest['close']
                change_pct = ((latest['close'] - previous['close']) / previous['close']) * 100
            elif len(symbol_df) == 1:
                price = symbol_df.iloc[0]['close']
                change_pct = 0
            else:
                continue

            # Determine sector
            sector = "Other"
            for sec, symbols in PORTFOLIO.items():
                if symbol in symbols:
                    sector = sec
                    break

            display_data.append({
                "Symbol": symbol,
                "Sector": sector,
                "Price": f"${price:.2f}",
                "Change": f"{change_pct:+.2f}%",
                "Change Value": change_pct,
            })

        if display_data:
            display_df = pd.DataFrame(display_data)

            # Color-code changes
            def color_change(val):
                if isinstance(val, str) and val.startswith('+'):
                    return "color: green"
                elif isinstance(val, str) and val.startswith('-'):
                    return "color: red"
                return ""

            # Display portfolio table
            st.dataframe(
                display_df.style.map(color_change, subset=["Change"]),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.warning("⚠️ No price data available. Click 'Refresh Data' to fetch.")
    else:
        st.warning("⚠️ No price data available. Click 'Refresh Data' to fetch.")

    st.divider()

    # =========================================================================
    # MIDDLE SECTION: Two columns
    # =========================================================================
    col1, col2 = st.columns(2)

    with col1:
        st.subheader(f"📈 {selected_symbol} Price History")

        history_df = get_price_history(selected_symbol, db, days=90)

        if not history_df.empty and 'date' in history_df.columns and len(history_df) >= 2:
            # Create chart
            chart_data = history_df.set_index('date')[['close', 'open']]
            chart_data.columns = ['Close', 'Open']

            st.line_chart(chart_data)

            # Show stats
            latest_price = history_df['close'].iloc[-1]
            oldest_price = history_df['close'].iloc[0]
            period_change = ((latest_price - oldest_price) / oldest_price) * 100

            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Latest Price", f"${latest_price:.2f}")
            col_b.metric("90-Day Change", f"{period_change:+.1f}%")
            col_c.metric("Data Points", len(history_df))
        elif not history_df.empty and len(history_df) == 1:
            latest_price = history_df['close'].iloc[0]
            st.metric("Latest Price", f"${latest_price:.2f}")
            st.info("Only 1 data point available — change calculation requires at least 2.")
        else:
            st.warning(f"No historical data for {selected_symbol}")

    with col2:
        st.subheader("📊 Economic Indicators")

        econ_df = get_economic_indicators(db)

        if not econ_df.empty:
            # Map series IDs to friendly names
            friendly_names = {
                "VIXCLS": "VIX (Volatility)",
                "DGS10": "10Y Treasury",
                "T10Y2Y": "Yield Curve (10Y-2Y)",
                "FEDFUNDS": "Fed Funds Rate",
                "UNRATE": "Unemployment Rate",
                "CPIAUCSL": "CPI",
                "GDP": "GDP",
            }

            # Display key indicators
            key_indicators = ["VIXCLS", "DGS10", "T10Y2Y", "FEDFUNDS"]

            for series_id in key_indicators:
                row = econ_df[econ_df['series_id'] == series_id]
                if not row.empty:
                    value = row['value'].iloc[-1]
                    date = row['date'].iloc[-1]
                    name = friendly_names.get(series_id, series_id)

                    # Format value
                    if series_id in ["VIXCLS", "DGS10", "T10Y2Y", "FEDFUNDS"]:
                        formatted_value = f"{value:.2f}"
                        if series_id == "DGS10" or series_id == "FEDFUNDS":
                            formatted_value += "%"
                    else:
                        formatted_value = f"{value:.3f}"

                    st.metric(name, formatted_value, delta=f"As of {date}")
        else:
            st.warning("⚠️ No economic data available")

    st.divider()

    # =========================================================================
    # BOTTOM SECTION: SEC Filings
    # =========================================================================
    st.subheader(f"📄 SEC Filings - {selected_symbol}")

    filings_df = get_sec_filings(selected_symbol, db)

    if not filings_df.empty:
        # Display filings table
        display_filings = filings_df[['report_type', 'filing_date', 'report_url']].copy()
        display_filings.columns = ['Type', 'Filing Date', 'URL']

        # Make URLs clickable
        display_filings['URL'] = display_filings['URL'].apply(
            lambda x: f"[View]({x})" if pd.notna(x) else ""
        )

        st.markdown(display_filings.to_markdown(index=False), unsafe_allow_html=True)
    else:
        st.warning(f"No SEC filings found for {selected_symbol}")

    # =========================================================================
    # FOOTER
    # =========================================================================
    st.divider()
    st.caption(
        f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Data source: OpenBB (Yahoo Finance, FRED, SEC EDGAR)"
    )


if __name__ == "__main__":
    main()
