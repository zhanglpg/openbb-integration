"""Integration tests: verify Streamlit pages load without import errors.

Regression tests for:
  - ImportError: cannot import name 'WATCHLIST' from 'config'
  - ImportError: cannot import name 'ECONOMIC_INDICATORS' from 'config'

Root cause: dashboard.py and pages/2_Economy.py imported from config before
importing shared.py (which adds src/ to sys.path).
"""

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure src/ is on the path (conftest.py also does this)
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Root of the project (needed so ``import shared`` and ``import dashboard`` resolve)
_PROJECT_ROOT = str(Path(__file__).parent.parent)


def _fresh_import(module_name: str, module_file: str | None = None):
    """Import *module_name* from scratch, removing any cached version first.

    This ensures import-order side-effects (like sys.path manipulation in
    shared.py) are exercised exactly as they would be when Streamlit loads the
    page for the first time.
    """
    # Remove previously cached modules so the import is truly fresh
    for key in list(sys.modules):
        if key in (module_name, "shared", "config", "economic_dashboard"):
            del sys.modules[key]

    if module_file:
        # For pages that live in a subdirectory, use importlib to load by path
        spec = importlib.util.spec_from_file_location(module_name, module_file)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    else:
        return importlib.import_module(module_name)


@pytest.fixture
def _mock_streamlit():
    """Mock streamlit so page modules can be imported without a running server."""
    mock_st = MagicMock()
    mock_st.columns.return_value = [MagicMock() for _ in range(4)]
    mock_st.sidebar.columns.return_value = [MagicMock(), MagicMock()]
    mock_st.sidebar.selectbox.return_value = "AAPL"
    mock_st.cache_data = MagicMock(return_value=lambda f: f)
    mock_st.cache_resource = MagicMock(return_value=lambda f: f)

    mock_sortables = MagicMock()
    mock_sortables.sort_items = MagicMock(side_effect=lambda items, **kw: items)

    with patch.dict(sys.modules, {
        "streamlit": mock_st,
        "streamlit.components": MagicMock(),
        "streamlit.components.v1": MagicMock(),
        "streamlit_sortables": mock_sortables,
    }):
        yield mock_st


@pytest.mark.integration
class TestDashboardPageLoads:
    """dashboard.py must import without errors."""

    def test_dashboard_imports_successfully(self, _mock_streamlit):
        """Importing dashboard.py should not raise ImportError."""
        # Ensure the project root is on sys.path so ``import dashboard`` works
        if _PROJECT_ROOT not in sys.path:
            sys.path.insert(0, _PROJECT_ROOT)

        mod = _fresh_import("dashboard")

        # Verify key objects exist after import
        assert hasattr(mod, "WATCHLIST")
        assert isinstance(mod.WATCHLIST, dict)
        assert len(mod.WATCHLIST) > 0

        assert hasattr(mod, "PORTFOLIO")
        assert isinstance(mod.PORTFOLIO, dict)

        assert hasattr(mod, "ALL_SYMBOLS")
        assert isinstance(mod.ALL_SYMBOLS, list)
        assert len(mod.ALL_SYMBOLS) > 0

    def test_watchlist_categories_present(self, _mock_streamlit):
        """WATCHLIST should contain the expected category keys."""
        if _PROJECT_ROOT not in sys.path:
            sys.path.insert(0, _PROJECT_ROOT)

        mod = _fresh_import("dashboard")

        for category in ("tech", "china", "semiconductors", "etfs"):
            assert category in mod.WATCHLIST, f"Missing WATCHLIST category: {category}"


@pytest.mark.integration
class TestEconomyPageLoads:
    """pages/2_Economy.py must import without errors."""

    def test_economy_page_imports_successfully(self, _mock_streamlit):
        """Importing 2_Economy.py should not raise ImportError.

        The page calls main() at module level, so we patch it out to test
        that the import chain (shared -> config -> economic_dashboard) works.
        """
        if _PROJECT_ROOT not in sys.path:
            sys.path.insert(0, _PROJECT_ROOT)

        economy_file = str(Path(__file__).parent.parent / "pages" / "2_Economy.py")

        # Patch main() to a no-op so we only test import resolution
        with patch("builtins.__import__", wraps=__import__):
            # Read the source, replace the top-level main() call, then exec
            source = Path(economy_file).read_text()
            # Remove the trailing main() call so we only test imports
            source = source.replace("\nmain()\n", "\n")
            source = source.rstrip()
            if source.endswith("main()"):
                source = source[: -len("main()")]

            code = compile(source, economy_file, "exec")
            mod_dict: dict = {}
            exec(code, mod_dict)  # noqa: S102

        # Verify the config import worked
        from config import ECONOMIC_INDICATORS

        assert isinstance(ECONOMIC_INDICATORS, dict)
        assert len(ECONOMIC_INDICATORS) > 0

    def test_economic_indicators_series_present(self, _mock_streamlit):
        """ECONOMIC_INDICATORS should contain expected FRED series."""
        from config import ECONOMIC_INDICATORS

        for series_id in ("GDP", "UNRATE", "CPIAUCSL", "FEDFUNDS", "VIXCLS"):
            assert series_id in ECONOMIC_INDICATORS, (
                f"Missing ECONOMIC_INDICATORS series: {series_id}"
            )
