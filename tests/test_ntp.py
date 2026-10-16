"""
Tests for NTP time verification.
"""

from unittest.mock import MagicMock, patch


def test_check_ntp_time_returns_true_when_offset_acceptable():
    """check_ntp_time returns True when system time is within threshold."""
    from app.ntp import check_ntp_time

    mock_response = MagicMock()
    mock_response.offset = 0.5
    mock_response.tx_time = 1700000000.0

    with patch("app.ntp.ntplib.NTPClient") as mock_ntp_client:
        mock_client = MagicMock()
        mock_client.request.return_value = mock_response
        mock_ntp_client.return_value = mock_client

        result = check_ntp_time(threshold_sec=60)

    assert result is True


def test_check_ntp_time_returns_false_when_offset_exceeds_threshold():
    """check_ntp_time returns False and logs error when offset exceeds threshold."""
    from app.ntp import check_ntp_time

    mock_response = MagicMock()
    mock_response.offset = 120.0
    mock_response.tx_time = 1700000000.0

    with patch("app.ntp.ntplib.NTPClient") as mock_ntp_client:
        mock_client = MagicMock()
        mock_client.request.return_value = mock_response
        mock_ntp_client.return_value = mock_client

        with patch("app.ntp.logger") as mock_logger:
            result = check_ntp_time(threshold_sec=60)

    assert result is False
    mock_logger.error.assert_called_once()
    assert "incorrect" in mock_logger.error.call_args[0][0].lower()


def test_check_ntp_time_returns_true_when_ntp_unreachable():
    """check_ntp_time returns True (proceed) when NTP request fails."""
    from app.ntp import check_ntp_time

    with patch("app.ntp.ntplib.NTPClient") as mock_ntp_client:
        mock_client = MagicMock()
        mock_client.request.side_effect = OSError("Connection refused")
        mock_ntp_client.return_value = mock_client

        with patch("app.ntp.logger") as mock_logger:
            result = check_ntp_time()

    assert result is True
    mock_logger.warning.assert_called_once()
    assert "Could not verify" in mock_logger.warning.call_args[0][0]


def test_check_ntp_time_returns_true_when_ntplib_missing():
    """check_ntp_time returns True when ntplib is not installed."""
    from app import ntp

    orig_ntplib = ntp.ntplib
    try:
        ntp.ntplib = None
        from app.ntp import check_ntp_time

        with patch("app.ntp.logger") as mock_logger:
            result = check_ntp_time()

        assert result is True
        mock_logger.warning.assert_called_once()
        assert "not installed" in mock_logger.warning.call_args[0][0]
    finally:
        ntp.ntplib = orig_ntplib


def test_ntp_is_healthy_reflects_status():
    """ntp_is_healthy is True only for the healthy status."""
    from app.ntp import ntp_is_healthy

    with patch("app.ntp._state", {"status": "healthy"}):
        assert ntp_is_healthy() is True
    with patch("app.ntp._state", {"status": "unhealthy"}):
        assert ntp_is_healthy() is False
    with patch("app.ntp._state", {"status": "unknown"}):
        assert ntp_is_healthy() is False


def test_check_once_marks_healthy_when_offset_within_threshold():
    """_check_once marks healthy when the offset is within threshold."""
    from app.ntp import _check_once

    state = {"status": "unknown"}
    with patch("app.ntp._state", state):
        with patch("app.ntp._query_offset", return_value=0.5):
            _check_once()

    assert state["status"] == "healthy"
    assert state["offset_sec"] == 0.5


def test_check_once_marks_unhealthy_when_offset_exceeds_threshold():
    """_check_once marks unhealthy when the offset exceeds the threshold."""
    from app.ntp import _check_once

    state = {"status": "healthy"}
    with patch("app.ntp._state", state):
        with patch("app.ntp._query_offset", return_value=120.0):
            _check_once()

    assert state["status"] == "unhealthy"
    assert state["offset_sec"] == 120.0


def test_check_once_marks_unknown_when_query_fails():
    """_check_once marks unknown (and records the error) when the query fails."""
    from app.ntp import _check_once

    state = {"status": "healthy"}
    with patch("app.ntp._state", state):
        with patch("app.ntp._query_offset", side_effect=OSError("unreachable")):
            _check_once()

    assert state["status"] == "unknown"
    assert state["last_error"] == "unreachable"


def test_start_ntp_monitor_starts_daemon_thread():
    """start_ntp_monitor launches a daemon thread."""
    from app.ntp import start_ntp_monitor

    with patch("app.ntp.threading.Thread") as mock_thread:
        start_ntp_monitor()

    mock_thread.assert_called_once()
    assert mock_thread.call_args.kwargs["daemon"] is True
    mock_thread.return_value.start.assert_called_once()
