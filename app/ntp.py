"""
NTP time verification and continuous monitoring.

The container shares the host clock, so it cannot correct the time itself.
Instead it continuously verifies the clock against NTP and tracks a health
state (``unknown``, ``healthy``, ``unhealthy``). Archive and retention jobs
gate on ``healthy``, so data is only written or removed when the clock is
known-good.
"""

from __future__ import annotations

import logging
import threading
import time

try:
    import ntplib
except ImportError:
    ntplib = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

# Default NTP server when none is configured.
_DEFAULT_NTP_SERVER = "time.cloudflare.com"

# Max allowed offset (seconds) between system UTC and NTP before marking unhealthy
_NTP_OFFSET_THRESHOLD_SEC = 10

# Bound on a single NTP check. ntplib's socket timeout does not cover its
# socket.getaddrinfo lookup, which can hang for minutes on a broken resolver,
# so the whole request is run in a worker thread with this join timeout.
_NTP_CHECK_TIMEOUT_SEC = 10.0

# How often the monitor re-checks NTP while the container runs.
_NTP_MONITOR_INTERVAL_SEC = 300

_lock = threading.Lock()
_state = {
    "status": "unknown",  # unknown | healthy | unhealthy
    "offset_sec": None,  # float | None, last measured offset
    "last_check_ts": None,  # float | None, time.time() of last completed check
    "last_error": None,  # str | None, last failure reason
    "server": _DEFAULT_NTP_SERVER,
    "threshold_sec": _NTP_OFFSET_THRESHOLD_SEC,
    "interval_sec": _NTP_MONITOR_INTERVAL_SEC,
    # Bumped whenever the configured server changes, so a check that started
    # against an older server cannot publish a stale result.
    "generation": 0,
}


def _query_offset(
    server: str = _DEFAULT_NTP_SERVER,
    timeout_sec: float = _NTP_CHECK_TIMEOUT_SEC,
) -> float:
    """Return the clock offset from ``server``, bounded to ``timeout_sec``."""
    if ntplib is None:
        raise RuntimeError("ntplib not installed")

    result: dict = {}

    def _run() -> None:
        try:
            client = ntplib.NTPClient()
            response = client.request(server.strip(), version=3)
            result["offset"] = float(response.offset)
        except Exception as exc:
            result["error"] = exc

    thread = threading.Thread(target=_run, daemon=True, name="ntp-query")
    thread.start()
    thread.join(timeout_sec)
    if thread.is_alive():
        raise TimeoutError(f"NTP check exceeded {timeout_sec:.0f}s")
    if "error" in result:
        raise result["error"]
    return result["offset"]


def check_ntp_time(
    server: str = _DEFAULT_NTP_SERVER,
    threshold_sec: float = _NTP_OFFSET_THRESHOLD_SEC,
) -> bool:
    """
    One-shot check: True if the clock is within threshold, or NTP is unreachable.

    For gating archive and retention work, prefer the continuous monitor via
    ``ntp_is_healthy`` instead.
    """
    if ntplib is None:
        logger.warning("ntplib not installed; skipping NTP time verification.")
        return True
    try:
        offset = _query_offset(server)
    except Exception as exc:
        logger.warning(
            "Could not verify system time via NTP (%s): %s. "
            "Proceeding without time verification.",
            server,
            exc,
        )
        return True
    if abs(offset) > threshold_sec:
        logger.error(
            "System time is incorrect: UTC offset from NTP (%s) is %.1f s "
            "(threshold %.1f s).",
            server,
            offset,
            threshold_sec,
        )
        return False
    logger.debug("NTP check OK: UTC offset %.2f s from %s", offset, server)
    return True


def _norm_server(server) -> str:
    """Coerce the configured server to a usable string, else the default."""
    if isinstance(server, str):
        server = server.strip()
        if server:
            return server
    return _DEFAULT_NTP_SERVER


def _snapshot() -> tuple[str, int]:
    """Return the current (server, generation), owned by the config path."""
    with _lock:
        return _state["server"], _state.get("generation", 0)


def _check_result(server: str, generation: int, status: str, offset, error) -> None:
    """Publish a result only when its server is still the current one."""
    with _lock:
        if _state["server"] != server or _state.get("generation", 0) != generation:
            return
        previous = _state["status"]
        _state["status"] = status
        _state["offset_sec"] = offset
        _state["last_check_ts"] = time.time()
        _state["last_error"] = error
    if status != previous:
        logger.info("NTP health: %s -> %s", previous, status)


def _check_once(server=_DEFAULT_NTP_SERVER, generation: int | None = None) -> None:
    """Check ``server`` and publish the result if it is still current.

    The server selection is owned by the config path, so this worker never
    writes ``_state["server"]``. It runs the probe and reports under the
    generation it was launched for; a stale result is discarded.
    """
    server = _norm_server(server)
    if generation is None:
        generation = _snapshot()[1]
    try:
        offset = _query_offset(server)
    except Exception as exc:
        _check_result(server, generation, "unknown", None, str(exc))
        return
    if abs(offset) > _NTP_OFFSET_THRESHOLD_SEC:
        _check_result(server, generation, "unhealthy", offset, None)
    else:
        _check_result(server, generation, "healthy", offset, None)


def _monitor_loop(interval_sec: float = _NTP_MONITOR_INTERVAL_SEC) -> None:
    """Run a scheduled check of the currently configured server each pass.

    The server is owned by set_ntp_server; this loop only checks the current
    snapshot, so it cannot reinstate a server the configuration moved away from.
    """
    while True:
        cur_server, cur_generation = _snapshot()
        _check_once(cur_server, cur_generation)
        time.sleep(interval_sec)


def start_ntp_monitor(interval_sec: float = _NTP_MONITOR_INTERVAL_SEC) -> None:
    """Start the background NTP health monitor thread."""
    threading.Thread(
        target=_monitor_loop,
        args=(interval_sec,),
        daemon=True,
        name="ntp-monitor",
    ).start()


def set_ntp_server(server) -> None:
    """Switch the monitored server, clear stale results, and re-check now."""
    server = _norm_server(server)
    with _lock:
        if _state["server"] == server:
            return
        _state["generation"] = _state.get("generation", 0) + 1
        _state["server"] = server
        _state["status"] = "unknown"
        _state["offset_sec"] = None
        _state["last_error"] = None
        generation = _state["generation"]
    threading.Thread(
        target=_check_once, args=(server, generation), daemon=True, name="ntp-recheck"
    ).start()


def get_ntp_status() -> dict:
    """Return a snapshot of the NTP health state."""
    with _lock:
        return dict(_state)


def ntp_is_healthy() -> bool:
    """True only when the latest NTP check confirmed the clock is within threshold."""
    with _lock:
        return _state["status"] == "healthy"
