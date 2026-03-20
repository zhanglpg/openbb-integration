"""Tests for shared.py — Streamlit utility functions."""

import re
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Mock streamlit and openbb before importing shared
_mock_st = MagicMock()
_mock_st.session_state = {"theme": "dark"}
_mock_st.query_params = {}
sys.modules.setdefault("streamlit", _mock_st)
sys.modules.setdefault("openbb", MagicMock())

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import shared  # noqa: E402

# ===================================================================
# Color constants
# ===================================================================


@pytest.mark.unit
class TestColorConstants:
    def test_up_color_valid_hex(self):
        assert re.match(r"^#[0-9A-Fa-f]{6}$", shared.UP_COLOR)

    def test_down_color_valid_hex(self):
        assert re.match(r"^#[0-9A-Fa-f]{6}$", shared.DOWN_COLOR)

    def test_colors_dict_all_valid_hex(self):
        for name, color in shared.COLORS.items():
            assert re.match(r"^#[0-9A-Fa-f]{6}$", color), f"{name}: {color}"


# ===================================================================
# Theme palettes
# ===================================================================


@pytest.mark.unit
class TestThemePalettes:
    REQUIRED_KEYS = {"bg", "secondary_bg", "text", "grid", "zeroline", "hover_bg", "hover_border"}

    def test_dark_has_required_keys(self):
        assert self.REQUIRED_KEYS.issubset(set(shared.THEME_PALETTES["dark"].keys()))

    def test_light_has_required_keys(self):
        assert self.REQUIRED_KEYS.issubset(set(shared.THEME_PALETTES["light"].keys()))

    def test_dark_and_light_differ(self):
        assert shared.THEME_PALETTES["dark"]["bg"] != shared.THEME_PALETTES["light"]["bg"]


# ===================================================================
# area_fillcolor
# ===================================================================


@pytest.mark.unit
class TestAreaFillcolor:
    def test_red_conversion(self):
        result = shared.area_fillcolor("#FF0000", 0.5)
        assert result == "rgba(255,0,0,0.5)"

    def test_blue_conversion(self):
        result = shared.area_fillcolor("#0000FF", 0.3)
        assert result == "rgba(0,0,255,0.3)"

    def test_default_opacity(self):
        result = shared.area_fillcolor("#123456")
        assert result == "rgba(18,52,86,0.15)"

    def test_full_opacity(self):
        result = shared.area_fillcolor("#FFFFFF", 1.0)
        assert result == "rgba(255,255,255,1.0)"

    def test_black(self):
        result = shared.area_fillcolor("#000000", 0.5)
        assert result == "rgba(0,0,0,0.5)"


# ===================================================================
# chart_config
# ===================================================================


@pytest.mark.unit
class TestChartConfig:
    def test_returns_dict(self):
        result = shared.chart_config()
        assert isinstance(result, dict)

    def test_has_scroll_zoom(self):
        result = shared.chart_config()
        assert "scrollZoom" in result


# ===================================================================
# apply_chart_defaults
# ===================================================================


@pytest.mark.unit
class TestApplyChartDefaults:
    def test_sets_height(self):
        import plotly.graph_objects as go

        fig = go.Figure()
        shared.apply_chart_defaults(fig, height=500)
        assert fig.layout.height == 500

    def test_default_height(self):
        import plotly.graph_objects as go

        fig = go.Figure()
        shared.apply_chart_defaults(fig)
        assert fig.layout.height == 350

    def test_skip_weekends_adds_rangebreaks(self):
        import plotly.graph_objects as go

        fig = go.Figure()
        shared.apply_chart_defaults(fig, skip_weekends=True)
        rangebreaks = fig.layout.xaxis.rangebreaks
        assert len(rangebreaks) == 1
        assert rangebreaks[0].bounds == ("sat", "mon")

    def test_no_skip_weekends(self):
        import plotly.graph_objects as go

        fig = go.Figure()
        shared.apply_chart_defaults(fig, skip_weekends=False)
        # rangebreaks should not be set
        rangebreaks = fig.layout.xaxis.rangebreaks
        assert rangebreaks is None or len(rangebreaks) == 0

    def test_returns_figure(self):
        import plotly.graph_objects as go

        fig = go.Figure()
        result = shared.apply_chart_defaults(fig)
        assert result is fig


# ===================================================================
# get_theme / get_palette
# ===================================================================


@pytest.mark.unit
class TestGetTheme:
    def test_returns_string(self):
        result = shared.get_theme()
        assert isinstance(result, str)
        assert result in ("dark", "light")

    def test_get_palette_returns_dict(self):
        palette = shared.get_palette()
        assert isinstance(palette, dict)
        assert "bg" in palette
        assert "text" in palette
