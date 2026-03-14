#!/usr/bin/env python3
"""
Streamlit Portfolio Dashboard
Real-time portfolio monitoring with OpenBB data pipeline
"""

import streamlit as st
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from database import Database
from run_pipeline import run_full_pipeline

# Page config
st.set_page_config(
    page_title="Portfolio Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Your portfolio holdings
PORTFOLIO = {
    "Tech": ["GOOGL", "NVDA"],
    "Semiconductors": ["TSMC"],
    "China Internet": ["BABA"],
    "US Broad Market": ["SPY"],
    "China Indices": ["FXI", "KWEB"],
}

ALL_SYMBOLS = ["GOOGL", "NVDA", "TSMC", "BABA", "SPY", "FXI", "KWEB"]


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_latest_prices(db: Database) -> pd.DataFrame:
    """Get latest prices for all portfolio symbols."""
    all_prices = []
    
    for symbol in ALL_SYMBOLS:
        df = db.get_latest_prices(symbol, days=1)
        if not df.empty:
            all_prices.append(df.iloc[-1])
    
    if not all_prices:
        return pd.DataFrame()
    
    return pd.DataFrame(all_prices)


@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_price_history(symbol: str, db: Database, days: int = 90) -> pd.DataFrame:
    """Get price history for a symbol."""
    df = db.get_latest_prices(symbol, days=days)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
    return df


@st.cache_data(ttl=3600)
def get_economic_indicators(db: Database) -> pd.DataFrame:
    """Get latest economic indicators."""
    query = """
    SELECT series_id, date, value
    FROM economic_indicators e1
    WHERE date = (
        SELECT MAX(date) FROM economic_indicators e2 
        WHERE e2.series_id = e1.series_id
    )
    ORDER BY series_id
    """
    with sqlite3.connect(db.db_path) as conn:
        return pd.read_sql_query(query, conn)


@st.cache_data(ttl=3600)
def get_sec_filings(symbol: str, db: Database, limit: int = 10) -> pd.DataFrame:
    """Get latest SEC filings for a symbol."""
    import glob
    
    pattern = str(db.sec_dir / f"{symbol}_sec_filings_*.parquet")
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
    
    # Initialize database
    db = Database()
    
    # =========================================================================
    # TOP SECTION: Portfolio Overview
    # =========================================================================
    st.subheader("Portfolio Overview")
    
    prices_df = get_latest_prices(db)
    
    if not prices_df.empty:
        # Calculate portfolio metrics
        total_value = 0
        total_change = 0
        
        # Create display dataframe
        display_data = []
        for symbol in ALL_SYMBOLS:
            row = prices_df[prices_df['symbol'] == symbol]
            if not row.empty:
                row = row.iloc[-1]
                price = row.get('close', 0)
                change_pct = row.get('change_percent', 0)
                
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
            display_df.style.applymap(color_change, subset=["Change"]),
            use_container_width=True,
            hide_index=True
        )
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
        
        if not history_df.empty and 'date' in history_df.columns:
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
