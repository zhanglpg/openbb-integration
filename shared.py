"""Shared utilities for multi-page Streamlit dashboard."""

import sys
from pathlib import Path

# Add src to path for all pages
sys.path.insert(0, str(Path(__file__).parent / "src"))

import streamlit as st

from database import Database
from run_pipeline import run_full_pipeline


@st.cache_resource
def get_db():
    """Cached database connection (persists across Streamlit reruns)."""
    return Database()


def render_sidebar_controls():
    """Render shared sidebar controls (Refresh Data, Reset Cache)."""
    st.sidebar.header("Controls")

    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("🔄 Refresh Data", use_container_width=True):
            with st.spinner("Fetching latest data..."):
                try:
                    run_full_pipeline()
                    st.success("✅ Data refreshed successfully!")
                except Exception as e:
                    st.error(f"❌ Error refreshing data: {str(e)}")
            st.rerun()

    with col2:
        if st.button("🔃 Reset Cache", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    st.sidebar.divider()
