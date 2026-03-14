"""Tests for src/watchlist.py."""

import pytest
from pathlib import Path

from watchlist import WatchlistManager


class TestWatchlistManager:
    def test_load_from_file(self, tmp_watchlist_file):
        wl = WatchlistManager(watchlist_path=str(tmp_watchlist_file))
        assert wl.get_symbols() == ["AAPL", "MSFT", "GOOGL"]

    def test_load_missing_file(self, tmp_path):
        wl = WatchlistManager(watchlist_path=str(tmp_path / "nonexistent.txt"))
        assert wl.get_symbols() == []

    def test_load_ignores_comments_and_blanks(self, tmp_path):
        wl_file = tmp_path / "watchlist.txt"
        wl_file.write_text("# Comment\n\nAAPL\n\n# Another comment\nMSFT\n")
        wl = WatchlistManager(watchlist_path=str(wl_file))
        assert wl.get_symbols() == ["AAPL", "MSFT"]

    def test_save_creates_file(self, tmp_path):
        wl_file = tmp_path / "sub" / "watchlist.txt"
        wl = WatchlistManager(watchlist_path=str(wl_file))
        wl.symbols = ["AAPL", "TSLA"]
        wl.save()
        assert wl_file.exists()
        content = wl_file.read_text()
        assert "AAPL" in content
        assert "TSLA" in content

    def test_add_symbol(self, tmp_watchlist_file):
        wl = WatchlistManager(watchlist_path=str(tmp_watchlist_file))
        result = wl.add("TSLA")
        assert result is True
        assert "TSLA" in wl.get_symbols()

    def test_add_duplicate_symbol(self, tmp_watchlist_file):
        wl = WatchlistManager(watchlist_path=str(tmp_watchlist_file))
        result = wl.add("AAPL")
        assert result is False

    def test_add_normalizes_case(self, tmp_watchlist_file):
        wl = WatchlistManager(watchlist_path=str(tmp_watchlist_file))
        wl.add("tsla")
        assert "TSLA" in wl.get_symbols()

    def test_remove_symbol(self, tmp_watchlist_file):
        wl = WatchlistManager(watchlist_path=str(tmp_watchlist_file))
        result = wl.remove("AAPL")
        assert result is True
        assert "AAPL" not in wl.get_symbols()

    def test_remove_nonexistent_symbol(self, tmp_watchlist_file):
        wl = WatchlistManager(watchlist_path=str(tmp_watchlist_file))
        result = wl.remove("TSLA")
        assert result is False

    def test_get_symbols_returns_copy(self, tmp_watchlist_file):
        wl = WatchlistManager(watchlist_path=str(tmp_watchlist_file))
        symbols = wl.get_symbols()
        symbols.append("HACKED")
        assert "HACKED" not in wl.get_symbols()

    def test_get_symbol_set(self, tmp_watchlist_file):
        wl = WatchlistManager(watchlist_path=str(tmp_watchlist_file))
        symbol_set = wl.get_symbol_set()
        assert isinstance(symbol_set, set)
        assert "AAPL" in symbol_set

    def test_len(self, tmp_watchlist_file):
        wl = WatchlistManager(watchlist_path=str(tmp_watchlist_file))
        assert len(wl) == 3

    def test_iter(self, tmp_watchlist_file):
        wl = WatchlistManager(watchlist_path=str(tmp_watchlist_file))
        symbols = list(wl)
        assert symbols == ["AAPL", "MSFT", "GOOGL"]
