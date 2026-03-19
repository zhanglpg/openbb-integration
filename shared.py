"""Shared utilities for multi-page Streamlit dashboard."""

import sys
from datetime import datetime, timezone
from pathlib import Path

# Add src to path for all pages
sys.path.insert(0, str(Path(__file__).parent / "src"))

import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

from database import Database
from run_pipeline import run_full_pipeline

# ---------------------------------------------------------------------------
# Color constants — single source of truth for all dashboard pages
# ---------------------------------------------------------------------------
UP_COLOR = "#26a69a"
DOWN_COLOR = "#ef5350"

COLORS = {
    "blue": "#2196F3",
    "orange": "#FF9800",
    "green": "#26a69a",
    "red": "#ef5350",
    "purple": "#9C27B0",
    "pink": "#E91E63",
    "teal": "#4ECDC4",
    "salmon": "#FF6B6B",
    "sky": "#45B7D1",
    "mint": "#96CEB4",
    "cream": "#FFEAA7",
    "plum": "#DDA0DD",
    "bb_orange": "#FF9800",
}

SERIES_COLORS = [COLORS["blue"], COLORS["orange"], COLORS["green"]]

# ---------------------------------------------------------------------------
# Plotly dark template — auto-applies to every go.Figure()
# ---------------------------------------------------------------------------
_openclaw_template = go.layout.Template(
    layout=go.Layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#FAFAFA"),
        xaxis=dict(
            gridcolor="rgba(128,128,128,0.15)",
            zerolinecolor="rgba(128,128,128,0.25)",
        ),
        yaxis=dict(
            gridcolor="rgba(128,128,128,0.15)",
            zerolinecolor="rgba(128,128,128,0.25)",
        ),
        hoverlabel=dict(bgcolor="#1A1D23", font_color="#FAFAFA", bordercolor="#333"),
        margin=dict(l=40, r=20, t=40, b=30),
        legend=dict(font=dict(color="#FAFAFA")),
    )
)
pio.templates["openclaw_dark"] = _openclaw_template
pio.templates.default = "openclaw_dark"


# ---------------------------------------------------------------------------
# Chart helpers
# ---------------------------------------------------------------------------
def apply_chart_defaults(fig: go.Figure, height: int = 350, skip_weekends: bool = False):
    """Apply standard layout defaults to a Plotly figure."""
    fig.update_layout(height=height)
    if skip_weekends:
        fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])
    return fig


def chart_config() -> dict:
    """Standard Plotly chart config dict."""
    return {"scrollZoom": True}


def area_fillcolor(hex_color: str, opacity: float = 0.15) -> str:
    """Convert a hex color to an rgba string for dark-compatible area fills."""
    r = int(hex_color[1:3], 16)
    g = int(hex_color[3:5], 16)
    b = int(hex_color[5:7], 16)
    return f"rgba({r},{g},{b},{opacity})"


# ---------------------------------------------------------------------------
# Global responsive CSS — call once per page
# ---------------------------------------------------------------------------
def inject_global_css():
    """Inject responsive CSS for metric cards, columns, dividers, dataframes."""
    st.markdown(
        """<style>
        @media (max-width: 768px) {
            [data-testid="stMetricValue"] { font-size: 1.1rem !important; }
            [data-testid="stMetricLabel"] { font-size: 0.75rem !important; }
            [data-testid="column"] { min-width: 0 !important; padding: 0 4px !important; }
            [data-testid="stHorizontalBlock"] {
                flex-wrap: wrap !important;
            }
            [data-testid="stHorizontalBlock"] > [data-testid="column"] {
                flex: 0 0 48% !important;
                min-width: 48% !important;
            }
        }
        @media (max-width: 480px) {
            [data-testid="stMetricValue"] { font-size: 0.95rem !important; }
            [data-testid="stMetricLabel"] { font-size: 0.7rem !important; }
            .stTabs [data-baseweb="tab"] { font-size: 0.85rem !important; }
        }
        </style>""",
        unsafe_allow_html=True,
    )


@st.cache_resource
def get_db():
    """Cached database connection (persists across Streamlit reruns)."""
    return Database()


def _time_ago(dt: datetime) -> str:
    """Return a human-readable relative time string."""
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = now - dt
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return "just now"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    days = hours // 24
    return f"{days}d ago"


def _freshness_color(dt: datetime) -> str:
    """Return a color hex based on data staleness."""
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    hours = (now - dt).total_seconds() / 3600
    if hours < 1:
        return "#26a69a"  # green — fresh
    elif hours < 24:
        return "#FF9800"  # orange — aging
    return "#ef5350"  # red — stale


@st.cache_data(ttl=60)
def get_data_freshness(_db) -> dict:
    """Query fetch_log for the most recent successful fetch per data type.

    Returns a dict mapping data_type -> datetime (UTC) or None.
    """
    import sqlite3

    freshness: dict[str, datetime | None] = {}
    data_types = ["prices", "fundamentals", "sec_filings", "economic"]
    try:
        with sqlite3.connect(_db.db_path) as conn:
            for dt in data_types:
                row = conn.execute(
                    "SELECT MAX(fetch_date) FROM fetch_log "
                    "WHERE data_type = ? AND status = 'success'",
                    (dt,),
                ).fetchone()
                if row and row[0]:
                    freshness[dt] = datetime.fromisoformat(row[0])
                else:
                    freshness[dt] = None
    except Exception:
        for dt in data_types:
            freshness.setdefault(dt, None)
    return freshness


def render_freshness_sidebar(_db) -> None:
    """Render data freshness status in the sidebar."""
    freshness = get_data_freshness(_db)
    labels = {
        "prices": "Prices",
        "fundamentals": "Fundamentals",
        "sec_filings": "SEC Filings",
        "economic": "Economic",
    }
    lines = []
    for key, label in labels.items():
        dt = freshness.get(key)
        if dt is None:
            lines.append(f":{label} — :gray[never synced]")
        else:
            color = _freshness_color(dt)
            ago = _time_ago(dt)
            # Use inline HTML for colored dot
            lines.append(f'<span style="color:{color}">●</span> {label} — {ago}')
    st.sidebar.markdown("**Data Freshness**")
    st.sidebar.markdown("<br>".join(lines), unsafe_allow_html=True)


def render_sidebar_controls():
    """Render shared sidebar controls (Refresh Data, Reset Cache)."""
    st.sidebar.header("Controls")

    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("🔄 Refresh Data", width="stretch"):
            with st.spinner("Fetching latest data..."):
                try:
                    run_full_pipeline()
                    st.success("✅ Data refreshed successfully!")
                except Exception as e:
                    st.error(f"❌ Error refreshing data: {str(e)}")
            st.rerun()

    with col2:
        if st.button("🔃 Reset Cache", width="stretch"):
            st.cache_data.clear()
            st.rerun()

    # Show data freshness below controls
    render_freshness_sidebar(get_db())

    st.sidebar.divider()
