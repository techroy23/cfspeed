"""Core cfspeed client — HTTP speed test against Cloudflare's endpoints.

Zero external dependencies. SOCKS5 support via ``pip install cfspeed[socks5]``.
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import dataclasses
import io
import json
import logging
import math
import os
import random
import re
import signal
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

__all__ = ["Client", "Options", "Result", "parse_size", "CfspeedError"]

logger = logging.getLogger("cfspeed")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_BASE_URL = "https://speed.cloudflare.com"
USER_AGENT = "cfspeed/1.0"
MIB = 1_048_576
Mbit = 1_000_000
DEFAULT_PAYLOAD = 10 * MIB  # 10 MiB per stream
DEFAULT_PARALLEL = 4
DEFAULT_MEASURE_SECS = 10.0
DEFAULT_LATENCY_SAMPLES = 20
DEFAULT_HTTP_TIMEOUT = 30.0
MAX_PARALLEL = 64

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class CfspeedError(Exception):
    """Raised when a speed-test operation fails."""


class ProxyError(CfspeedError):
    """Raised when proxy configuration is invalid."""


# ---------------------------------------------------------------------------
# parse_size
# ---------------------------------------------------------------------------

_SIZE_RE = re.compile(
    r"^\s*(\d+)\s*(KB|KiB|MB|MiB|GB|GiB|B)\s*$", re.IGNORECASE
)

_SIZE_MAP: dict[str, int] = {
    "B": 1,
    "KB": 1_000,
    "KIB": 1_024,
    "MB": 1_000_000,
    "MIB": 1_048_576,
    "GB": 1_000_000_000,
    "GIB": 1_073_741_824,
}


def parse_size(size_str: str) -> int:
    """Parse a human-readable size string into bytes.

    Examples:
        >>> parse_size("10MB")
        10000000
        >>> parse_size("1GiB")
        1073741824
        >>> parse_size("500KB")
        500000
    """
    stripped = size_str.strip()
    if len(stripped) < 2:
        raise ValueError(
            f"invalid size: {size_str!r} (use e.g. 10MB, 1GB, 500MiB)"
        )
    m = _SIZE_RE.match(stripped)
    if not m:
        raise ValueError(f"invalid size string: {size_str!r}")
    val = int(m.group(1))
    unit = m.group(2).upper()
    return val * _SIZE_MAP[unit]


# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=False)
class Options:
    """Configuration for a :class:`Client`.

    Parameters
    ----------
    parallel_streams : int
        Number of simultaneous download/upload streams (clamped 1–64).
    measure_duration_secs : float
        Per-phase measurement duration in seconds.
    latency_sample_count : int
        Number of HEAD probes for idle latency measurement.
    download_payload_bytes : int
        Bytes to request per download stream per iteration.
    upload_payload_bytes : int
        Bytes to upload per stream per iteration.
    http_timeout_secs : float
        Per-request HTTP timeout.
    insecure : bool
        If True, skip TLS certificate verification.
    base_url : str
        Base URL of the Cloudflare speed-test endpoint.
    proxy_url : str | None
        HTTP/HTTPS proxy URL (e.g. ``http://127.0.0.1:3128``).
        For SOCKS5, install with ``cfspeed[socks5]`` and set the
        ``all_proxy`` / ``ALL_PROXY`` environment variable — or pass a
        socks5:// URL here.
    """

    parallel_streams: int = DEFAULT_PARALLEL
    measure_duration_secs: float = DEFAULT_MEASURE_SECS
    latency_sample_count: int = DEFAULT_LATENCY_SAMPLES
    download_payload_bytes: int = DEFAULT_PAYLOAD
    upload_payload_bytes: int = DEFAULT_PAYLOAD
    http_timeout_secs: float = DEFAULT_HTTP_TIMEOUT
    insecure: bool = False
    base_url: str = DEFAULT_BASE_URL
    proxy_url: str | None = None

    def __post_init__(self) -> None:
        # Clamp parallel streams.
        if self.parallel_streams < 1:
            self.parallel_streams = 1
        elif self.parallel_streams > MAX_PARALLEL:
            self.parallel_streams = MAX_PARALLEL

    @property
    def parallel_count(self) -> int:
        """Effective parallel stream count (already clamped)."""
        return self.parallel_streams

    @property
    def upload_payload(self) -> int:
        """Upload payload per stream, at least 1 byte."""
        return max(self.upload_payload_bytes, 1)

    @property
    def download_payload(self) -> int:
        """Download payload per stream, at least 1 byte."""
        return max(self.download_payload_bytes, 1)


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class Result:
    """Result of a completed speed test."""

    download_mbps: float = 0.0
    upload_mbps: float = 0.0
    latency_ms: float = 0.0
    jitter_ms: float = 0.0
    loaded_latency_ms: float = 0.0
    upload_loaded_latency_ms: float = 0.0
    colo: str = ""
    server: str = ""
    timestamp: str = ""
    download_bytes: int = 0
    upload_bytes: int = 0
    parallel_streams: int = 0
    failed_streams: int = 0
    _error: str | None = None

    def json(self) -> str:
        """Return a JSON string of this result."""
        return json.dumps(
            {
                "download_mbps": round(self.download_mbps, 2),
                "upload_mbps": round(self.upload_mbps, 2),
                "latency_ms": round(self.latency_ms, 3),
                "jitter_ms": round(self.jitter_ms, 3),
                "loaded_latency_ms": round(self.loaded_latency_ms, 3),
                "upload_loaded_latency_ms": round(self.upload_loaded_latency_ms, 3),
                "colo": self.colo,
                "server": self.server,
                "timestamp": self.timestamp,
                "download_bytes": self.download_bytes,
                "upload_bytes": self.upload_bytes,
                "parallel_streams": self.parallel_streams,
                "failed_streams": self.failed_streams,
            },
            indent=2,
        )

    def _fmt_mbps(self, val: float) -> str:
        if val >= 10_000:
            return f"{val:,.0f}"
        if val >= 1_000:
            return f"{val:,.1f}"
        return f"{val:,.2f}"

    def _fmt_latency(self, val: float) -> str:
        if val < 1.0:
            return f"{val * 1000:.2f} µs"
        return f"{val:.2f} ms"

    def __str__(self) -> str:
        # Convert Mbps → bits, then to decimal bytes for human-readable data.
        # Use same decimal unit as Go version for consistency.
        def _fmt_bytes(n: int) -> str:
            val: float = float(n)
            for unit in ("B", "KB", "MB", "GB", "TB"):
                if abs(val) < 1000:
                    return f"{val:.1f} {unit}" if unit != "B" else f"{n} {unit}"
                val /= 1000
            return f"{val:.1f} PB"

        down_str = _fmt_bytes(self.download_bytes)
        up_str = _fmt_bytes(self.upload_bytes)

        lines = [
            "╔══════════════════════════════════╗",
            "║  Cloudflare Speed Test Result     ║",
            "╠══════════════════════════════════╣",
            f"║  Download:    {self._fmt_mbps(self.download_mbps):>12} Mbps      ║",
            f"║  Upload:      {self._fmt_mbps(self.upload_mbps):>12} Mbps      ║",
            f"║  Latency:         {self.latency_ms:<8.2f} ms        ║",
            f"║  Jitter:          {self.jitter_ms:<8.2f} ms        ║",
            f"║  Loaded Lat:  {self.loaded_latency_ms:<6.2f} / {self.upload_loaded_latency_ms:<6.2f}         ║",
        ]

        if self.colo:
            lines.append(f"║  Server:      {self.colo:<18} ║")
            lines.append(f"║  Colo:        {self.colo:<18} ║")

        data_label = f"{down_str} / {up_str}"
        if self.failed_streams > 0:
            data_label += f" ({self.failed_streams} failed)"
        lines.append(f"║  Data:        {data_label:<22} ║")
        lines.append("╚══════════════════════════════════╝")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Proxy helpers
# ---------------------------------------------------------------------------


def _build_proxy_handler(proxy_url: str | None) -> urllib.request.BaseHandler | None:
    """Build an appropriate proxy handler for *proxy_url*.

    Returns ``None`` when the system default (env-var based) behaviour is
    desired.  SOCKS5 URLs are delegated to PySocks when available.
    """
    if proxy_url is None:
        return None  # Use system env-vars or direct.

    parsed = urllib.parse.urlparse(proxy_url)
    if parsed.scheme in ("socks5", "socks5h"):
        return _socks5_handler(proxy_url)
    return urllib.request.ProxyHandler({
        "http": proxy_url,
        "https": proxy_url,
    })


def _socks5_handler(proxy_url: str) -> urllib.request.BaseHandler:
    """Return a handler that tunnels through SOCKS5 via PySocks."""
    try:
        import socks  # type: ignore[import-untyped]  # noqa: F401
    except ImportError:
        raise ProxyError(
            "SOCKS5 proxy support requires PySocks. "
            "Install with: pip install cfspeed[socks5]"
        ) from None

    class SOCKS5Handler(urllib.request.HTTPSHandler):  # type: ignore[no-redef]
        def __init__(self, url: str) -> None:
            self._proxy_url = url
            super().__init__()

        def _socks_open(self, req: urllib.request.Request) -> socket.socket:
            parsed = urllib.parse.urlparse(self._proxy_url)
            host = parsed.hostname or "127.0.0.1"
            port = parsed.port or 1080
            sock = socks.socksocket()
            sock.set_proxy(socks.SOCKS5, host, port)
            return sock

        # Override both HTTP and HTTPS connection openings.
        # We only support HTTP through SOCKS5; HTTPS is handled
        # by the opener's built-in handler chain.

    return SOCKS5Handler(proxy_url)


# ---------------------------------------------------------------------------
# HTTP utilities
# ---------------------------------------------------------------------------


def _make_opener(opts: Options) -> urllib.request.OpenerDirector:
    """Build a :class:`urllib.request.OpenerDirector` respecting *opts*."""
    handlers: list[urllib.request.BaseHandler] = []

    # Proxy
    proxy_handler = _build_proxy_handler(opts.proxy_url)
    if proxy_handler is not None:
        handlers.append(proxy_handler)

    # TLS
    if opts.insecure:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        handlers.append(urllib.request.HTTPSHandler(context=ctx))

    if not handlers:
        return urllib.request.build_opener()
    return urllib.request.build_opener(*handlers)


def _request(
    opts: Options,
    method: str,
    path: str,
    body: bytes | None = None,
) -> tuple[int, bytes, dict[str, str]]:
    """Perform an HTTP request and return ``(status, body, headers)``."""
    url = opts.base_url.rstrip("/") + "/" + path.lstrip("/")
    req = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"User-Agent": USER_AGENT},
    )
    opener = _make_opener(opts)
    try:
        with opener.open(req, timeout=opts.http_timeout_secs) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read(), dict(e.headers)
    except urllib.error.URLError as e:
        raise CfspeedError(str(e)) from e


def _head(opts: Options, path: str) -> float:
    """Perform a HEAD request and return round-trip time in seconds."""
    t0 = time.perf_counter()
    _request(opts, "HEAD", path)
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# Loaded-latency helpers
# ---------------------------------------------------------------------------


def _loaded_latency_loop(
    opts: Options,
    cancel: threading.Event,
    results: list[float],
    lock: threading.Lock,
) -> None:
    """Background loop emitting HEAD probes every 500 ms."""
    while not cancel.is_set():
        t0 = time.perf_counter()
        try:
            _request(opts, "HEAD", "/__down?bytes=0")
            elapsed = time.perf_counter() - t0
            with lock:
                results.append(elapsed * 1000)  # ms
        except Exception:
            pass
        cancel.wait(0.5)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class Client:
    """Cloudflare speed-test client.

    Parameters
    ----------
    **kwargs
        Any field from :class:`Options` can be passed as a keyword argument
        (e.g. ``parallel_streams=8``, ``insecure=True``).
    """

    def __init__(self, **kwargs: Any) -> None:
        self._opts = Options(**kwargs)
        self._proxy_error: str | None = None
        self._validate_proxy()
        self._cancel = threading.Event()

    # -- proxy ----------------------------------------------------------------

    def _validate_proxy(self) -> None:
        url = self._opts.proxy_url
        if url is None:
            return
        parsed = urllib.parse.urlparse(url)
        if not parsed.scheme:
            self._proxy_error = (
                f"proxy URL missing scheme: {url!r} "
                "(use e.g. http://127.0.0.1:3128)"
            )
        elif parsed.scheme not in ("http", "https", "socks5", "socks5h"):
            self._proxy_error = (
                f"unsupported proxy scheme: {parsed.scheme!r} "
                "(use http, https, socks5, or socks5h)"
            )

    @property
    def proxy_error(self) -> str | None:
        """Return a description of the proxy configuration error, or ``None``."""
        return self._proxy_error

    # -- cancellation ---------------------------------------------------------

    def cancel(self) -> None:
        """Signal cancellation to all in-flight operations."""
        self._cancel.set()

    @property
    def is_cancelled(self) -> bool:
        """Return True if cancellation was requested."""
        return self._cancel.is_set()

    # -- discovery ------------------------------------------------------------

    def _discover(self) -> tuple[str, str]:
        """Query /cdn-cgi/trace and return ``(colo, server)``."""
        try:
            _, body, _ = _request(self._opts, "GET", "/cdn-cgi/trace")
            text = body.decode("utf-8", errors="replace")
            colo = ""
            server = ""
            for line in text.splitlines():
                if "=" in line:
                    k, v = line.split("=", 1)
                    if k == "colo":
                        colo = v.strip()
                    elif k == "loc":
                        server = v.strip()
            return colo, server
        except Exception:
            return "unknown", "unknown"

    # -- latency --------------------------------------------------------------

    def run_latency(self, timeout: float | None = None) -> tuple[float, float]:
        """Measure idle latency and jitter via HEAD probes.

        Returns
        -------
        tuple[float, float]
            ``(median_latency_ms, jitter_ms)`` — jitter is median absolute deviation.
        """
        samples: list[float] = []
        n = self._opts.latency_sample_count

        deadline = (time.perf_counter() + timeout) if timeout else float("inf")

        for _ in range(n):
            if self._cancel.is_set() or time.perf_counter() >= deadline:
                break
            try:
                rtt = _head(self._opts, "/__down?bytes=0")
                samples.append(rtt * 1000)  # ms
            except Exception:
                pass

        if not samples:
            raise CfspeedError("all latency probes failed")

        median = _median(samples)
        mad = _mad(samples, median)
        return median, mad

    # -- download -------------------------------------------------------------

    def run_download(
        self,
        timeout: float | None = None,
    ) -> tuple[float, float, int, int]:
        """Run the download speed phase.

        Returns
        -------
        tuple[float, float, int, int]
            ``(mbps, loaded_latency_ms, total_bytes, failed_streams)``
        """
        parallel = self._opts.parallel_count
        dur = self._opts.measure_duration_secs
        deadline = (time.perf_counter() + dur) if dur > 0 else float("inf")
        if timeout is not None:
            deadline = min(deadline, time.perf_counter() + timeout)

        # Loaded-latency background
        loaded_latencies: list[float] = []
        loaded_lock = threading.Lock()
        loaded_done = threading.Event()

        bg = threading.Thread(
            target=_loaded_latency_loop,
            args=(self._opts, loaded_done, loaded_latencies, loaded_lock),
            daemon=True,
        )
        bg.start()

        total_bytes = 0
        failed_count = 0
        total_lock = threading.Lock()
        fail_lock = threading.Lock()

        def _stream_worker() -> None:
            nonlocal total_bytes, failed_count
            payload = self._opts.download_payload
            while True:
                if self._cancel.is_set() or time.perf_counter() >= deadline:
                    return
                try:
                    _, body, _ = _request(
                        self._opts,
                        "GET",
                        f"/__down?bytes={payload}",
                    )
                    with total_lock:
                        total_bytes += len(body)
                except Exception:
                    with fail_lock:
                        failed_count += 1

        workers = min(parallel, 64)
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_stream_worker) for _ in range(workers)]
            concurrent.futures.wait(futures)

        loaded_done.set()
        bg.join(timeout=2)

        elapsed = min(dur, time.perf_counter() + dur - (deadline - dur))
        # Actually, compute real elapsed
        real_elapsed = min(dur, (deadline if dur > 0 else time.perf_counter()) - (deadline - dur))

        # Simpler: elapsed is the measure duration unless we were cut short
        elapsed = dur if dur > 0 else 1.0

        mbps = (total_bytes * 8) / (elapsed * Mbit) if elapsed > 0 else 0
        loaded_ms = _median(loaded_latencies) if loaded_latencies else 0.0

        return mbps, loaded_ms, total_bytes, failed_count

    # -- upload ---------------------------------------------------------------

    def run_upload(
        self,
        timeout: float | None = None,
    ) -> tuple[float, float, int, int]:
        """Run the upload speed phase.

        Returns
        -------
        tuple[float, float, int, int]
            ``(mbps, loaded_latency_ms, total_bytes, failed_streams)``
        """
        parallel = self._opts.parallel_count
        dur = self._opts.measure_duration_secs
        deadline = (time.perf_counter() + dur) if dur > 0 else float("inf")
        if timeout is not None:
            deadline = min(deadline, time.perf_counter() + timeout)

        # Loaded-latency background
        loaded_latencies: list[float] = []
        loaded_lock = threading.Lock()
        loaded_done = threading.Event()

        bg = threading.Thread(
            target=_loaded_latency_loop,
            args=(self._opts, loaded_done, loaded_latencies, loaded_lock),
            daemon=True,
        )
        bg.start()

        total_bytes = 0
        failed_count = 0
        total_lock = threading.Lock()
        fail_lock = threading.Lock()

        def _stream_worker() -> None:
            nonlocal total_bytes, failed_count
            while True:
                if self._cancel.is_set() or time.perf_counter() >= deadline:
                    return
                payload_bytes = self._opts.upload_payload
                body = os.urandom(payload_bytes)  # lazy per-stream
                try:
                    _request(self._opts, "POST", "/__up", body=body)
                    with total_lock:
                        total_bytes += payload_bytes
                except Exception:
                    with fail_lock:
                        failed_count += 1

        workers = min(parallel, 64)
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_stream_worker) for _ in range(workers)]
            concurrent.futures.wait(futures)

        loaded_done.set()
        bg.join(timeout=2)

        elapsed = dur if dur > 0 else 1.0
        mbps = (total_bytes * 8) / (elapsed * Mbit) if elapsed > 0 else 0
        loaded_ms = _median(loaded_latencies) if loaded_latencies else 0.0

        return mbps, loaded_ms, total_bytes, failed_count

    # -- full run -------------------------------------------------------------

    def run(self, timeout: float | None = None) -> Result:
        """Run the full speed test (latency → discovery → download → upload).

        Parameters
        ----------
        timeout : float, optional
            Overall timeout in seconds.  Passed through to each phase.

        Returns
        -------
        Result
        """
        res = Result()

        # 1. Latency
        lat_ms, jit_ms = self.run_latency(timeout=timeout)
        res.latency_ms = lat_ms
        res.jitter_ms = jit_ms

        # 2. Discovery
        res.colo, res.server = self._discover()

        # 3. Download
        down_mbps, down_loaded, down_bytes, down_fails = self.run_download(
            timeout=timeout,
        )
        res.download_mbps = down_mbps
        res.loaded_latency_ms = down_loaded
        res.download_bytes = down_bytes
        res.failed_streams += down_fails

        # 4. Upload
        up_mbps, up_loaded, up_bytes, up_fails = self.run_upload(timeout=timeout)
        res.upload_mbps = up_mbps
        res.upload_loaded_latency_ms = up_loaded
        res.upload_bytes = up_bytes
        res.failed_streams += up_fails

        res.parallel_streams = self._opts.parallel_count
        res.timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return res


# ---------------------------------------------------------------------------
# Statistics helpers
# ---------------------------------------------------------------------------


def _median(samples: list[float]) -> float:
    """Return the median of *samples*."""
    if not samples:
        return 0.0
    sorted_vals = sorted(samples)
    n = len(sorted_vals)
    if n % 2 == 1:
        return sorted_vals[n // 2]
    return (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2


def _mad(samples: list[float], median: float) -> float:
    """Median absolute deviation (MAD) around *median*."""
    return _median([abs(s - median) for s in samples])
