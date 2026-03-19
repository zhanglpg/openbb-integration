"""Shared utilities for multi-page Streamlit dashboard."""

import sys
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

    st.sidebar.divider()
