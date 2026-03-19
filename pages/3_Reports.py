"""
Reports Page
Browse and generate daily market reports
"""

import subprocess  # noqa: I001
import sys

import streamlit as st

from shared import inject_global_css, render_sidebar_controls  # adds src/ to sys.path
from config import REPORTS_DIR


def get_available_reports() -> list[str]:
    """List report dates available in the reports directory."""
    if not REPORTS_DIR.exists():
        return []
    files = sorted(REPORTS_DIR.glob("*.md"), reverse=True)
    return [f.stem for f in files]


def load_report(date: str) -> str | None:
    """Load a report markdown file by date."""
    report_path = REPORTS_DIR / f"{date}.md"
    if report_path.exists():
        return report_path.read_text(encoding="utf-8")
    return None


def main():
    st.title("Daily Market Reports")
    st.markdown("**Automated daily briefings** | OpenClaw Financial Intelligence")

    inject_global_css()
    render_sidebar_controls()

    available = get_available_reports()

    col1, col2 = st.columns([3, 1])

    with col2:
        if st.button("Generate Now"):
            with st.spinner("Generating report..."):
                result = subprocess.run(
                    [sys.executable, "src/run_pipeline.py", "report"],
                    capture_output=True,
                    text=True,
                    cwd=str(REPORTS_DIR.parent.parent),
                    timeout=120,
                )
                if result.returncode == 0:
                    st.success("Report generated!")
                    st.rerun()
                else:
                    st.error(f"Error: {result.stderr[-500:] if result.stderr else 'Unknown error'}")

    with col1:
        if available:
            selected_date = st.selectbox(
                "Select report date",
                options=available,
                index=0,
            )
        else:
            selected_date = None
            st.info(
                'No reports available yet. Click "Generate Now" or '
                "run `python src/run_pipeline.py report` to create one."
            )

    if selected_date:
        st.divider()
        content = load_report(selected_date)
        if content:
            st.markdown(content)
        else:
            st.warning(f"Could not load report for {selected_date}")

    # Footer
    st.divider()
    st.caption(f"Reports stored in: {REPORTS_DIR} | {len(available)} report(s) available")


main()
