"""Tests for src/config.py."""

from config import (
    CACHE_DIR,
    DATA_DIR,
    DB_PATH,
    ECONOMIC_INDICATORS,
    PIPELINE_DEFAULTS,
    PROJECT_ROOT,
    SEC_FILING_TYPES,
    WATCHLIST,
)


class TestPaths:
    def test_project_root_is_directory(self):
        assert PROJECT_ROOT.is_dir()

    def test_data_dir_exists(self):
        assert DATA_DIR.exists()

    def test_cache_dir_exists(self):
        assert CACHE_DIR.exists()

    def test_db_path_is_under_data_dir(self):
        assert DB_PATH.parent == DATA_DIR


class TestWatchlist:
    def test_watchlist_has_categories(self):
        assert len(WATCHLIST) > 0

    def test_all_categories_have_symbols(self):
        for category, symbols in WATCHLIST.items():
            assert len(symbols) > 0, f"Category {category} is empty"

    def test_symbols_are_uppercase_strings(self):
        for category, symbols in WATCHLIST.items():
            for sym in symbols:
                assert isinstance(sym, str)
                assert sym == sym.upper(), f"{sym} should be uppercase"

    def test_known_categories_exist(self):
        assert "tech" in WATCHLIST
        assert "etfs" in WATCHLIST


class TestEconomicIndicators:
    def test_indicators_not_empty(self):
        assert len(ECONOMIC_INDICATORS) > 0

    def test_indicators_have_descriptions(self):
        for series_id, description in ECONOMIC_INDICATORS.items():
            assert isinstance(series_id, str)
            assert isinstance(description, str)
            assert len(description) > 0

    def test_known_series_exist(self):
        assert "GDP" in ECONOMIC_INDICATORS
        assert "UNRATE" in ECONOMIC_INDICATORS
        assert "VIXCLS" in ECONOMIC_INDICATORS


class TestSecFilingTypes:
    def test_filing_types_not_empty(self):
        assert len(SEC_FILING_TYPES) > 0

    def test_10k_and_10q_included(self):
        assert "10-K" in SEC_FILING_TYPES
        assert "10-Q" in SEC_FILING_TYPES


class TestPipelineDefaults:
    def test_all_values_positive(self):
        for key, value in PIPELINE_DEFAULTS.items():
            assert value > 0, f"Pipeline default {key}={value} must be positive"

    def test_known_defaults_exist(self):
        assert "price_lookback_days" in PIPELINE_DEFAULTS
        assert "sec_filing_limit" in PIPELINE_DEFAULTS
        assert "api_call_delay" in PIPELINE_DEFAULTS
        assert "max_retries" in PIPELINE_DEFAULTS
        assert "retry_backoff_base" in PIPELINE_DEFAULTS
