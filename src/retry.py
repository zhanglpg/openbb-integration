"""Retry utility for transient API failures."""

import logging
import time
from typing import Callable, TypeVar

from config import PIPELINE_DEFAULTS

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Exception types that indicate transient failures worth retrying
_TRANSIENT_MARKERS = (
    "timeout", "timed out", "connection", "429", "500", "502", "503", "rate limit",
)


def _is_transient(exc: Exception) -> bool:
    """Check if an exception looks transient (worth retrying)."""
    msg = str(exc).lower()
    return any(marker in msg for marker in _TRANSIENT_MARKERS)


def retry_fetch(
    fn: Callable[[], T],
    description: str = "API call",
    max_retries: int = None,
    backoff_base: float = None,
) -> T:
    """Call fn() with retry on transient failures.

    Args:
        fn: Zero-argument callable to execute.
        description: Human-readable label for log messages.
        max_retries: Number of retries (default from config).
        backoff_base: Base for exponential backoff in seconds (default from config).

    Returns:
        The return value of fn().

    Raises:
        The last exception if all retries are exhausted or the error is non-transient.
    """
    if max_retries is None:
        max_retries = PIPELINE_DEFAULTS["max_retries"]
    if backoff_base is None:
        backoff_base = PIPELINE_DEFAULTS["retry_backoff_base"]

    last_exc = None
    for attempt in range(1 + max_retries):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            if not _is_transient(e) or attempt >= max_retries:
                raise
            wait = backoff_base ** attempt
            logger.warning(
                "%s failed (attempt %d/%d): %s — retrying in %.1fs",
                description,
                attempt + 1,
                1 + max_retries,
                e,
                wait,
            )
            time.sleep(wait)

    raise last_exc  # unreachable, but makes type checkers happy
