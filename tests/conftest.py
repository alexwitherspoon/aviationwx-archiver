"""
Pytest configuration for AviationWX.org Archiver tests.

Patches time.sleep during tests so rate limiting does not slow test runs.
"""

from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _patch_rate_limit_sleep():
    """Disable rate-limit sleep in all tests."""
    with patch("app.archiver.time.sleep"):
        yield


@pytest.fixture(autouse=True)
def _patch_ntp_check():
    """Default the NTP health check to healthy so scheduler tests never block."""
    with patch("app.scheduler.ntp_is_healthy", return_value=True):
        yield
