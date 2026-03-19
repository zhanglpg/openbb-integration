"""
Economy Dashboard Page
Macroeconomic indicators and trends via OpenBB
"""

import pandas as pd  # noqa: I001
import streamlit as st

from shared import (  # must be first
    get_data_freshness,
    get_db,
    inject_global_css,
    render_sidebar_controls,
)

from config import ECONOMIC_INDICATORS
from economic_dashboard import EconomicDashboard


@st.cache_resource
def get_econ_dashboard():
    """Cached EconomicDashboard instance."""
    return EconomicDashboard()


@st.cache_data(ttl=3600)
def get_economic_indicators(_db) -> pd.DataFrame:
    """Get latest economic indicators from the database."""
    key_indicators = ["VIXCLS", "DGS10", "T10Y2Y", "FEDFUNDS"]
    return _db.get_latest_economic_indicators(key_indicators)


@st.cache_data(ttl=3600)
def get_gdp_real(_dashboard) -> pd.DataFrame | None:
    return _dashboard.fetch_gdp_real()


@st.cache_data(ttl=3600)
def get_gdp_nominal(_dashboard) -> pd.DataFrame | None:
    return _dashboard.fetch_gdp_nominal()


@st.cache_data(ttl=3600)
def get_cpi(_dashboard) -> pd.DataFrame | None:
    return _dashboard.fetch_cpi()


@st.cache_data(ttl=3600)
def get_unemployment(_dashboard) -> pd.DataFrame | None:
    return _dashboard.fetch_unemployment()


@st.cache_data(ttl=3600)
def get_interest_rates(_dashboard) -> pd.DataFrame | None:
    return _dashboard.fetch_interest_rates()


@st.cache_data(ttl=3600)
def get_fred_series(_dashboard, series_id: str) -> pd.DataFrame | None:
    return _dashboard.fetch_fred_series(series_id)


def render_chart(df: pd.DataFrame | None, value_col: str = "value", title: str = ""):
    """Render a line chart from a DataFrame, handling common column patterns."""
    if df is None or df.empty:
        st.warning(f"No data available for {title}")
        return

    # Try to find a date column for the index
    date_col = None
    for col in ["date", "Date", "period"]:
        if col in df.columns:
            date_col = col
            break

    # If date is the index, reset it
    if date_col is None and df.index.name in ["date", "Date", "period"]:
        df = df.reset_index()
        date_col = df.columns[0]

    # Find a value column
    val_col = None
    for col in [value_col, "value", "Value", "close"]:
        if col in df.columns:
            val_col = col
            break

    if val_col is None:
        # Use the first numeric column
        numeric_cols = df.select_dtypes(include="number").columns
        if len(numeric_cols) > 0:
            val_col = numeric_cols[0]
        else:
            st.warning(f"No numeric data found for {title}")
            return

    if date_col:
        chart_df = df[[date_col, val_col]].copy()
        chart_df[date_col] = pd.to_datetime(chart_df[date_col], errors="coerce")
        chart_df = chart_df.dropna(subset=[date_col]).sort_values(date_col)
        chart_df = chart_df.set_index(date_col)
    else:
        chart_df = df[[val_col]].copy()

    st.line_chart(chart_df)


def main():
    st.title("🏛️ Economy Dashboard")
    st.markdown("**Macroeconomic indicators and trends** | OpenBB Data Pipeline")

    inject_global_css()
    render_sidebar_controls()

    db = get_db()
    dashboard = get_econ_dashboard()

    # =========================================================================
    # KEY INDICATORS SUMMARY
    # =========================================================================
    st.subheader("Key Indicators")

    econ_df = get_economic_indicators(db)

    if not econ_df.empty:
        friendly_names = {
            "VIXCLS": "VIX (Volatility)",
            "DGS10": "10Y Treasury",
            "T10Y2Y": "Yield Curve (10Y-2Y)",
            "FEDFUNDS": "Fed Funds Rate",
        }

        cols = st.columns(4)
        for i, series_id in enumerate(["VIXCLS", "DGS10", "T10Y2Y", "FEDFUNDS"]):
            row = econ_df[econ_df["series_id"] == series_id]
            if not row.empty:
                value = row["value"].iloc[-1]
                date = row["date"].iloc[-1]
                name = friendly_names.get(series_id, series_id)
                if pd.isna(value):
                    cols[i].metric(name, "N/A", delta=f"As of {date}")
                    continue
                formatted = f"{value:.2f}"
                if series_id in ("DGS10", "FEDFUNDS"):
                    formatted += "%"
                cols[i].metric(name, formatted, delta=f"As of {date}")
    else:
        st.info("No indicator data available. Click 'Refresh Data' to fetch.")

    st.divider()

    # =========================================================================
    # GDP
    # =========================================================================
    st.subheader("📈 Gross Domestic Product")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Real GDP**")
        gdp_real = get_gdp_real(dashboard)
        render_chart(gdp_real, title="Real GDP")

    with col2:
        st.markdown("**Nominal GDP**")
        gdp_nominal = get_gdp_nominal(dashboard)
        render_chart(gdp_nominal, title="Nominal GDP")

    st.divider()

    # =========================================================================
    # INFLATION & EMPLOYMENT
    # =========================================================================
    st.subheader("📊 Inflation & Employment")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Consumer Price Index (CPI)**")
        cpi = get_cpi(dashboard)
        render_chart(cpi, title="CPI")

    with col2:
        st.markdown("**Unemployment Rate**")
        unemployment = get_unemployment(dashboard)
        render_chart(unemployment, title="Unemployment Rate")

    st.divider()

    # =========================================================================
    # INTEREST RATES
    # =========================================================================
    st.subheader("💰 Interest Rates")

    interest = get_interest_rates(dashboard)
    render_chart(interest, title="Interest Rates")

    st.divider()

    # =========================================================================
    # FRED SERIES EXPLORER
    # =========================================================================
    st.subheader("🔍 FRED Series Explorer")

    selected_series = st.selectbox(
        "Select FRED Series",
        options=list(ECONOMIC_INDICATORS.keys()),
        format_func=lambda x: f"{x} — {ECONOMIC_INDICATORS[x]}",
    )

    if selected_series:
        fred_data = get_fred_series(dashboard, selected_series)
        if fred_data is not None and not fred_data.empty:
            render_chart(fred_data, title=ECONOMIC_INDICATORS[selected_series])
        else:
            st.info(
                "No data returned. This series may require a FRED API key. "
                "Configure it in your OpenBB settings."
            )

    # =========================================================================
    # FOOTER
    # =========================================================================
    st.divider()
    freshness = get_data_freshness(db)
    econ_ts = freshness.get("economic")
    if econ_ts:
        synced_str = econ_ts.strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        synced_str = "never"
    st.caption(f"Data last synced: {synced_str} | Data source: OpenBB (FRED, World Bank)")


main()
