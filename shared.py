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
# Theme palettes
# ---------------------------------------------------------------------------
THEME_PALETTES = {
    "dark": {
        "bg": "#0E1117",
        "secondary_bg": "#262730",
        "text": "#FAFAFA",
        "grid": "rgba(128,128,128,0.15)",
        "zeroline": "rgba(128,128,128,0.25)",
        "hover_bg": "#1A1D23",
        "hover_border": "#333",
        "chart_grid": "#2a2a2a",
        "bar_text": "#FAFAFA",
    },
    "light": {
        "bg": "#FFFFFF",
        "secondary_bg": "#F0F2F6",
        "text": "#1A1A2E",
        "grid": "rgba(128,128,128,0.2)",
        "zeroline": "rgba(128,128,128,0.3)",
        "hover_bg": "#FFFFFF",
        "hover_border": "#CCC",
        "chart_grid": "#E0E0E0",
        "bar_text": "#1A1A2E",
    },
}


def get_theme() -> str:
    """Return current theme, persisted via query param ``t``."""
    if "theme" not in st.session_state:
        st.session_state["theme"] = st.query_params.get("t", "dark")
    return st.session_state["theme"]


def get_palette() -> dict:
    """Return the active theme palette."""
    return THEME_PALETTES[get_theme()]


# ---------------------------------------------------------------------------
# Plotly templates — dark and light variants
# ---------------------------------------------------------------------------
def _build_plotly_template(palette: dict, transparent: bool = True) -> go.layout.Template:
    bg = "rgba(0,0,0,0)" if transparent else palette["bg"]
    return go.layout.Template(
        layout=go.Layout(
            paper_bgcolor=bg,
            plot_bgcolor=bg,
            font=dict(color=palette["text"]),
            xaxis=dict(
                gridcolor=palette["grid"],
                zerolinecolor=palette["zeroline"],
            ),
            yaxis=dict(
                gridcolor=palette["grid"],
                zerolinecolor=palette["zeroline"],
            ),
            hoverlabel=dict(
                bgcolor=palette["hover_bg"],
                font_color=palette["text"],
                bordercolor=palette["hover_border"],
            ),
            margin=dict(l=40, r=20, t=40, b=30),
            legend=dict(font=dict(color=palette["text"])),
        )
    )


pio.templates["openclaw_dark"] = _build_plotly_template(THEME_PALETTES["dark"], transparent=True)
pio.templates["openclaw_light"] = _build_plotly_template(THEME_PALETTES["light"], transparent=False)
pio.templates.default = "openclaw_dark"


def apply_theme():
    """Set the default Plotly template based on current theme."""
    theme = get_theme()
    pio.templates.default = f"openclaw_{theme}"


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
    palette = get_palette()
    light_overrides = ""
    if get_theme() == "light":
        bg = palette["bg"]
        sbg = palette["secondary_bg"]
        txt = palette["text"]
        light_overrides = f"""
        /* Force browser-level light rendering */
        * {{ color-scheme: light !important; }}

        /* Streamlit CSS custom properties */
        .stApp {{
            background-color: {bg} !important;
            color: {txt} !important;
            color-scheme: light !important;
            --primary-color: #2196F3;
            --background-color: {bg};
            --secondary-background-color: {sbg};
            --text-color: {txt};
        }}

        /* Header / toolbar */
        [data-testid="stHeader"] {{
            background-color: {bg} !important;
            color: {txt} !important;
        }}
        .stDeployButton,
        [data-testid="stToolbar"] {{
            color: {txt} !important;
        }}

        /* Main content containers */
        [data-testid="stAppViewContainer"],
        .stAppViewBlockContainer,
        [data-testid="stBottomBlockContainer"] {{
            background-color: {bg} !important;
            color: {txt} !important;
        }}

        /* Sidebar */
        section[data-testid="stSidebar"],
        [data-testid="stSidebar"] {{
            background-color: {sbg} !important;
            color: {txt} !important;
        }}
        [data-testid="stSidebar"] *,
        [data-testid="stSidebarNavItems"] *,
        [data-testid="stSidebarNavLink"] * {{
            color: {txt} !important;
        }}
        [data-testid="stSidebarNavItems"],
        [data-testid="stSidebarNavLink"] {{
            color: {txt} !important;
        }}

        /* Text elements */
        h1, h2, h3, h4, h5, h6, p, span, label, div {{
            color: {txt} !important;
        }}
        [data-testid="stMarkdownContainer"],
        .stMarkdown, .stCaption {{
            color: {txt} !important;
        }}

        /* Metrics */
        [data-testid="stMetricValue"],
        [data-testid="stMetricLabel"],
        [data-testid="stMetricDelta"] {{
            color: {txt} !important;
        }}

        /* Expander */
        [data-testid="stExpander"] {{
            background-color: {sbg} !important;
            border-color: #D0D0D0 !important;
        }}

        /* Tabs */
        .stTabs [data-baseweb="tab"] {{
            color: {txt} !important;
        }}

        /* Form inputs */
        input, textarea, select, [data-baseweb="select"] {{
            background-color: {bg} !important;
            color: {txt} !important;
        }}

        /* Selectbox / multiselect */
        [data-testid="stSelectbox"],
        [data-testid="stMultiSelect"] {{
            color: {txt} !important;
        }}

        /* Dropdown menus / popovers */
        [data-baseweb="popover"],
        [data-baseweb="menu"],
        [data-baseweb="list"] {{
            background-color: {bg} !important;
            color: {txt} !important;
        }}
        [data-baseweb="popover"] *,
        [data-baseweb="menu"] *,
        [data-baseweb="list"] * {{
            color: {txt} !important;
        }}

        /* Dataframe / glide-data-grid */
        [data-testid="stDataFrame"] {{
            color: {txt} !important;
        }}
        [data-testid="stDataFrame"] iframe {{
            color-scheme: light !important;
        }}
        [data-testid="stDataFrameGlideDataEditor"] {{
            color-scheme: light !important;
        }}

        /* Alert boxes */
        [data-testid="stAlert"] {{
            color: {txt} !important;
        }}

        /* Selectbox / input internals (baseui) */
        [data-baseweb="select"] > div {{
            background-color: {bg} !important;
            color: {txt} !important;
            border-color: #CCC !important;
        }}
        [data-baseweb="input"] {{
            background-color: {bg} !important;
            color: {txt} !important;
        }}
        [data-baseweb="input"] > div {{
            background-color: {bg} !important;
        }}

        /* Buttons */
        .stButton > button {{
            background-color: {sbg} !important;
            color: {txt} !important;
            border-color: #CCC !important;
        }}
        [data-testid="stSidebar"] .stButton > button {{
            background-color: {bg} !important;
            color: {txt} !important;
            border-color: #CCC !important;
        }}

        /* Radio buttons / checkboxes */
        [data-testid="stRadio"] label,
        [data-testid="stCheckbox"] label {{
            color: {txt} !important;
        }}

        /* Plotly chart containers — override dark iframe background */
        .stPlotlyChart,
        [data-testid="stPlotlyChart"] {{
            background-color: {bg} !important;
        }}
        .stPlotlyChart iframe {{
            background-color: {bg} !important;
        }}

        /* Data editor */
        [data-testid="stDataEditor"] {{
            color-scheme: light !important;
        }}

        /* Info / warning / success boxes */
        .stAlert, [data-testid="stAlert"] {{
            background-color: {sbg} !important;
        }}

        /* Toggle */
        [data-testid="stToggle"] label span {{
            color: {txt} !important;
        }}
        """

    st.markdown(
        f"""<style>
        {light_overrides}
        @media (max-width: 768px) {{
            [data-testid="stMetricValue"] {{ font-size: 1.1rem !important; }}
            [data-testid="stMetricLabel"] {{ font-size: 0.75rem !important; }}
            [data-testid="column"] {{ min-width: 0 !important; padding: 0 4px !important; }}
            [data-testid="stHorizontalBlock"] {{
                flex-wrap: wrap !important;
            }}
            [data-testid="stHorizontalBlock"] > [data-testid="column"] {{
                flex: 0 0 48% !important;
                min-width: 48% !important;
            }}
        }}
        @media (max-width: 480px) {{
            [data-testid="stMetricValue"] {{ font-size: 0.95rem !important; }}
            [data-testid="stMetricLabel"] {{ font-size: 0.7rem !important; }}
            .stTabs [data-baseweb="tab"] {{ font-size: 0.85rem !important; }}
        }}
        </style>""",
        unsafe_allow_html=True,
    )


def _apply_streamlit_theme():
    """Dynamically override Streamlit's config.toml theme colors.

    Must run BEFORE any widgets render so Glide Data Grid picks up correct colors.
    """
    from streamlit import _config

    palette = get_palette()
    _config.set_option("theme.backgroundColor", palette["bg"])
    _config.set_option("theme.secondaryBackgroundColor", palette["secondary_bg"])
    _config.set_option("theme.textColor", palette["text"])


def setup_page_theme():
    """Single call to set up theming on every page."""
    _apply_streamlit_theme()
    render_theme_toggle()
    inject_global_css()
    apply_theme()


def symbol_selectbox(symbols, sidebar=True, label="Select Symbol"):
    """Shared symbol selector with cross-page persistence.

    Uses ``index=`` instead of ``key=`` to avoid bidirectional binding
    that conflicts with manual session_state writes from query params.
    """
    query_sym = st.query_params.get("symbol")
    current = st.session_state.get("selected_symbol")

    if query_sym and query_sym in symbols:
        default = query_sym
    elif current and current in symbols:
        default = current
    else:
        default = symbols[0]

    idx = symbols.index(default) if default in symbols else 0
    fn = st.sidebar.selectbox if sidebar else st.selectbox
    symbol = fn(label, symbols, index=idx)
    st.session_state["selected_symbol"] = symbol
    st.query_params["symbol"] = symbol
    return symbol


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


def render_theme_toggle():
    """Render light/dark toggle in top-right corner of main content."""
    _, right = st.columns([9, 1])
    with right:
        light_on = st.toggle("☀️", value=get_theme() == "light", key="light_mode_toggle")
        new_theme = "light" if light_on else "dark"
        if new_theme != st.session_state.get("theme"):
            st.session_state["theme"] = new_theme
        st.query_params["t"] = new_theme


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
