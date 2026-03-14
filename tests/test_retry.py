"""Tests for src/retry.py — retry logic for transient API failures."""

from unittest.mock import MagicMock, patch

import pytest

from retry import _is_transient, retry_fetch


class TestIsTransient:
    """Unit tests for _is_transient() detection."""

    @pytest.mark.parametrize(
        "message",
        [
            "Connection timed out",
            "Request timeout after 30s",
            "ConnectionError: host unreachable",
            "HTTP 429 Too Many Requests",
            "Server error 500",
            "Bad Gateway 502",
            "Service Unavailable 503",
            "Rate limit exceeded",
        ],
    )
    def test_detects_transient_errors(self, message):
        assert _is_transient(Exception(message)) is True

    @pytest.mark.parametrize(
        "message",
        [
            "KeyError: 'close'",
            "ValueError: invalid literal",
            "TypeError: unsupported operand",
            "FileNotFoundError: no such file",
            "HTTP 404 Not Found",
            "HTTP 401 Unauthorized",
        ],
    )
    def test_rejects_non_transient_errors(self, message):
        assert _is_transient(Exception(message)) is False


class TestRetryFetch:
    """Tests for retry_fetch() behavior."""

    def test_succeeds_first_attempt(self):
        fn = MagicMock(return_value="data")
        result = retry_fetch(fn, "test call", max_retries=3, backoff_base=2)
        assert result == "data"
        assert fn.call_count == 1

    @patch("retry.time.sleep")
    def test_retries_on_transient_then_succeeds(self, mock_sleep):
        fn = MagicMock(side_effect=[Exception("Connection timed out"), "data"])
        result = retry_fetch(fn, "test call", max_retries=3, backoff_base=2)
        assert result == "data"
        assert fn.call_count == 2
        mock_sleep.assert_called_once()

    @patch("retry.time.sleep")
    def test_raises_after_max_retries_exhausted(self, mock_sleep):
        fn = MagicMock(side_effect=Exception("Connection timeout"))
        with pytest.raises(Exception, match="Connection timeout"):
            retry_fetch(fn, "test call", max_retries=2, backoff_base=1)
        # 1 initial + 2 retries = 3 calls
        assert fn.call_count == 3
        assert mock_sleep.call_count == 2

    def test_raises_immediately_on_non_transient(self):
        fn = MagicMock(side_effect=ValueError("bad data"))
        with pytest.raises(ValueError, match="bad data"):
            retry_fetch(fn, "test call", max_retries=3, backoff_base=2)
        assert fn.call_count == 1  # no retry

    @patch("retry.time.sleep")
    def test_exponential_backoff(self, mock_sleep):
        fn = MagicMock(
            side_effect=[Exception("timeout"), Exception("timeout"), Exception("timeout"), "data"]
        )
        result = retry_fetch(fn, "test call", max_retries=3, backoff_base=2)
        assert result == "data"
        # backoff: 2^0=1, 2^1=2, 2^2=4
        assert mock_sleep.call_args_list[0][0][0] == 1.0  # 2**0
        assert mock_sleep.call_args_list[1][0][0] == 2.0  # 2**1
        assert mock_sleep.call_args_list[2][0][0] == 4.0  # 2**2

    @patch("retry.time.sleep")
    def test_uses_config_defaults(self, mock_sleep):
        """When max_retries/backoff_base not provided, uses PIPELINE_DEFAULTS."""
        fn = MagicMock(side_effect=[Exception("timeout"), "data"])
        result = retry_fetch(fn, "test call")
        assert result == "data"
        # Default backoff_base=2, attempt 0 -> sleep(2**0) = sleep(1)
        mock_sleep.assert_called_once_with(1.0)

    @patch("retry.time.sleep")
    def test_zero_retries_raises_immediately(self, mock_sleep):
        fn = MagicMock(side_effect=Exception("Connection timeout"))
        with pytest.raises(Exception, match="Connection timeout"):
            retry_fetch(fn, "test call", max_retries=0, backoff_base=2)
        assert fn.call_count == 1
        mock_sleep.assert_not_called()
