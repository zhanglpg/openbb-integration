"""Tests for data freshness helpers in shared.py."""

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from shared import _freshness_color, _time_ago, get_data_freshness

_INSERT_FETCH = (
    "INSERT INTO fetch_log"
    " (data_type, symbol, provider, fetch_date, status, record_count)"
    " VALUES (?, ?, ?, ?, ?, ?)"
)


# ---------------------------------------------------------------------------
# _time_ago
# ---------------------------------------------------------------------------
class TestTimeAgo:
    def _dt(self, **kwargs):
        """Return a UTC datetime offset from now."""
        return datetime.now(timezone.utc) - timedelta(**kwargs)

    def test_just_now(self):
        assert _time_ago(self._dt(seconds=10)) == "just now"

    def test_minutes(self):
        assert _time_ago(self._dt(minutes=5)) == "5m ago"

    def test_hours(self):
        assert _time_ago(self._dt(hours=3)) == "3h ago"

    def test_days(self):
        assert _time_ago(self._dt(days=2)) == "2d ago"

    def test_boundary_59_seconds(self):
        assert _time_ago(self._dt(seconds=59)) == "just now"

    def test_boundary_60_seconds(self):
        assert _time_ago(self._dt(seconds=60)) == "1m ago"

    def test_boundary_59_minutes(self):
        assert _time_ago(self._dt(minutes=59)) == "59m ago"

    def test_boundary_60_minutes(self):
        assert _time_ago(self._dt(minutes=60)) == "1h ago"

    def test_boundary_23_hours(self):
        assert _time_ago(self._dt(hours=23)) == "23h ago"

    def test_boundary_24_hours(self):
        assert _time_ago(self._dt(hours=24)) == "1d ago"

    def test_naive_datetime_treated_as_utc(self):
        naive = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=10)
        result = _time_ago(naive)
        assert result == "10m ago"


# ---------------------------------------------------------------------------
# _freshness_color
# ---------------------------------------------------------------------------
class TestFreshnessColor:
    GREEN = "#26a69a"
    ORANGE = "#FF9800"
    RED = "#ef5350"

    def _dt(self, **kwargs):
        return datetime.now(timezone.utc) - timedelta(**kwargs)

    def test_fresh_under_1h(self):
        assert _freshness_color(self._dt(minutes=30)) == self.GREEN

    def test_aging_between_1h_and_24h(self):
        assert _freshness_color(self._dt(hours=5)) == self.ORANGE

    def test_stale_over_24h(self):
        assert _freshness_color(self._dt(days=2)) == self.RED

    def test_boundary_just_under_1h(self):
        assert _freshness_color(self._dt(minutes=59)) == self.GREEN

    def test_boundary_just_over_1h(self):
        assert _freshness_color(self._dt(minutes=61)) == self.ORANGE

    def test_boundary_just_under_24h(self):
        assert _freshness_color(self._dt(hours=23)) == self.ORANGE

    def test_boundary_just_over_24h(self):
        assert _freshness_color(self._dt(hours=25)) == self.RED

    def test_naive_datetime(self):
        naive = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=10)
        assert _freshness_color(naive) == self.GREEN


# ---------------------------------------------------------------------------
# get_data_freshness
# ---------------------------------------------------------------------------
class TestGetDataFreshness:
    """Test get_data_freshness against a real tmp database."""

    @pytest.fixture(autouse=True)
    def _clear_cache(self):
        """Clear Streamlit cache before each test."""
        get_data_freshness.clear()

    def _insert(self, db, data_type, symbol, provider, ts, status, count):
        with sqlite3.connect(db.db_path) as conn:
            conn.execute(
                _INSERT_FETCH,
                (data_type, symbol, provider, ts, status, count),
            )
            conn.commit()

    def test_empty_fetch_log(self, tmp_db):
        result = get_data_freshness(tmp_db)
        assert result == {
            "prices": None,
            "fundamentals": None,
            "sec_filings": None,
            "economic": None,
        }

    def test_returns_latest_successful_fetch(self, tmp_db):
        self._insert(
            tmp_db,
            "prices",
            "AAPL",
            "yahoo",
            "2026-03-01 10:00:00",
            "success",
            5,
        )
        self._insert(
            tmp_db,
            "prices",
            "MSFT",
            "yahoo",
            "2026-03-19 14:00:00",
            "success",
            5,
        )
        result = get_data_freshness(tmp_db)
        assert result["prices"] == datetime.fromisoformat("2026-03-19 14:00:00")

    def test_ignores_failed_fetches(self, tmp_db):
        self._insert(
            tmp_db,
            "prices",
            "AAPL",
            "yahoo",
            "2026-03-19 14:00:00",
            "failed",
            0,
        )
        result = get_data_freshness(tmp_db)
        assert result["prices"] is None

    def test_multiple_data_types(self, tmp_db):
        self._insert(
            tmp_db,
            "prices",
            "AAPL",
            "yahoo",
            "2026-03-19 10:00:00",
            "success",
            5,
        )
        self._insert(
            tmp_db,
            "economic",
            "",
            "fred",
            "2026-03-18 08:00:00",
            "success",
            12,
        )
        result = get_data_freshness(tmp_db)
        assert result["prices"] == datetime.fromisoformat("2026-03-19 10:00:00")
        assert result["fundamentals"] is None
        assert result["sec_filings"] is None
        assert result["economic"] == datetime.fromisoformat("2026-03-18 08:00:00")

    def test_latest_wins_over_older(self, tmp_db):
        self._insert(
            tmp_db,
            "fundamentals",
            "AAPL",
            "yahoo",
            "2026-03-01 10:00:00",
            "success",
            1,
        )
        self._insert(
            tmp_db,
            "fundamentals",
            "AAPL",
            "yahoo",
            "2026-03-15 10:00:00",
            "success",
            1,
        )
        self._insert(
            tmp_db,
            "fundamentals",
            "MSFT",
            "yahoo",
            "2026-03-10 10:00:00",
            "success",
            1,
        )
        result = get_data_freshness(tmp_db)
        assert result["fundamentals"] == datetime.fromisoformat("2026-03-15 10:00:00")

    def test_success_after_failure(self, tmp_db):
        """A successful fetch after a failed one should be picked up."""
        self._insert(
            tmp_db,
            "sec_filings",
            "AAPL",
            "sec",
            "2026-03-19 12:00:00",
            "failed",
            0,
        )
        self._insert(
            tmp_db,
            "sec_filings",
            "AAPL",
            "sec",
            "2026-03-19 12:05:00",
            "success",
            3,
        )
        result = get_data_freshness(tmp_db)
        assert result["sec_filings"] == datetime.fromisoformat("2026-03-19 12:05:00")
