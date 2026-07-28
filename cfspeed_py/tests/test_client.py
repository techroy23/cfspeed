"""Tests for the cfspeed client."""

from __future__ import annotations

import contextlib
import json
import os
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
from typing import Iterator

import pytest

from cfspeed import Client, Options, Result, parse_size, CfspeedError


# ---------------------------------------------------------------------------
# Mock server
# ---------------------------------------------------------------------------


class MockHandler(BaseHTTPRequestHandler):
    """Simulates speed.cloudflare.com endpoints."""

    def do_GET(self) -> None:
        if self.path == "/cdn-cgi/trace":
            self._write(200, b"colo=MNL\nloc=PH\n")
        elif self.path.startswith("/__down"):
            import urllib.parse
            qs = urllib.parse.urlparse(self.path).query
            params = urllib.parse.parse_qs(qs)
            try:
                n = int(params.get("bytes", ["0"])[0])
            except (ValueError, IndexError):
                n = 0
            body = b"\x00" * n
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self._write(404, b"not found\n")

    def do_HEAD(self) -> None:
        if self.path.startswith("/__down"):
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self) -> None:
        if self.path == "/__up":
            length = int(self.headers.get("Content-Length", "0"))
            if length > 0:
                self.rfile.read(length)
            self.send_response(200)
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def _write(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, msg_format: str, *args: object) -> None:
        pass  # mute


@contextlib.contextmanager
def mock_server() -> Iterator[str]:
    server = HTTPServer(("127.0.0.1", 0), MockHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=2)


# ---------------------------------------------------------------------------
# Test parse_size
# ---------------------------------------------------------------------------


class TestParseSize:
    def test_mb(self) -> None:
        assert parse_size("10MB") == 10_000_000

    def test_mib(self) -> None:
        assert parse_size("10MiB") == 10_485_760

    def test_gb(self) -> None:
        assert parse_size("1GB") == 1_000_000_000

    def test_gib(self) -> None:
        assert parse_size("1GiB") == 1_073_741_824

    def test_kb(self) -> None:
        assert parse_size("500KB") == 500_000

    def test_kib(self) -> None:
        assert parse_size("512KiB") == 524_288

    def test_bytes(self) -> None:
        assert parse_size("100B") == 100

    def test_case_insensitive(self) -> None:
        assert parse_size("10mb") == 10_000_000
        assert parse_size("10mib") == 10_485_760

    def test_zero(self) -> None:
        assert parse_size("0MB") == 0

    def test_invalid_raises(self) -> None:
        for bad in ("", "10", "10XB", "abc"):
            with pytest.raises(ValueError):
                parse_size(bad)


# ---------------------------------------------------------------------------
# Test Options
# ---------------------------------------------------------------------------


class TestOptions:
    def test_parallel_clamp_min(self) -> None:
        opts = Options(parallel_streams=0)
        assert opts.parallel_count == 1

    def test_parallel_clamp_max(self) -> None:
        opts = Options(parallel_streams=999)
        assert opts.parallel_count == 64

    def test_parallel_negative(self) -> None:
        opts = Options(parallel_streams=-5)
        assert opts.parallel_count == 1

    def test_defaults(self) -> None:
        opts = Options()
        assert opts.parallel_count == 4
        assert opts.measure_duration_secs == 10.0


# ---------------------------------------------------------------------------
# Test Result
# ---------------------------------------------------------------------------


class TestResult:
    def test_json_contains_fields(self) -> None:
        r = Result(
            download_mbps=100.0,
            upload_mbps=50.0,
            latency_ms=0.5,
            jitter_ms=0.1,
            loaded_latency_ms=1.0,
            upload_loaded_latency_ms=1.5,
            colo="MNL",
            server="PH",
            timestamp="2026-01-01T00:00:00Z",
            download_bytes=1_000_000,
            upload_bytes=500_000,
            parallel_streams=4,
            failed_streams=0,
        )
        data = json.loads(r.json())
        assert data["download_mbps"] == 100.0
        assert data["failed_streams"] == 0
        assert data["colo"] == "MNL"

    def test_json_with_failed_streams(self) -> None:
        r = Result(failed_streams=3)
        data = json.loads(r.json())
        assert data["failed_streams"] == 3

    def test_string_output(self) -> None:
        r = Result(
            download_mbps=15000.0,
            upload_mbps=12000.0,
            latency_ms=0.1,
            jitter_ms=0.02,
            loaded_latency_ms=0.5,
            upload_loaded_latency_ms=0.6,
            colo="MNL",
            server="PH",
            download_bytes=1_000_000_000,
            upload_bytes=500_000_000,
            parallel_streams=4,
            failed_streams=2,
        )
        s = str(r)
        assert "Download:" in s
        assert "Upload:" in s
        assert "0.50 / 0.60" in s or "0.5" in s
        assert "2 failed" in s


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


class TestClient:
    def test_latency(self) -> None:
        with mock_server() as url:
            client = Client(
                base_url=url,
                latency_sample_count=5,
                http_timeout_secs=5,
            )
            lat_ms, jit_ms = client.run_latency()
            assert lat_ms >= 0
            assert jit_ms >= 0

    def test_download(self) -> None:
        with mock_server() as url:
            client = Client(
                base_url=url,
                parallel_streams=2,
                download_payload_bytes=1_000_000,
                measure_duration_secs=2,
                http_timeout_secs=10,
            )
            mbps, loaded, total_bytes, failed = client.run_download()
            assert mbps >= 0
            assert loaded >= 0
            assert total_bytes > 0
            assert failed >= 0

    def test_upload(self) -> None:
        with mock_server() as url:
            client = Client(
                base_url=url,
                parallel_streams=2,
                upload_payload_bytes=1_000_000,
                measure_duration_secs=2,
                http_timeout_secs=10,
            )
            mbps, loaded, total_bytes, failed = client.run_upload()
            assert mbps >= 0
            assert loaded >= 0
            assert total_bytes > 0
            assert failed >= 0

    def test_full_run(self) -> None:
        with mock_server() as url:
            client = Client(
                base_url=url,
                parallel_streams=2,
                download_payload_bytes=500_000,
                upload_payload_bytes=500_000,
                measure_duration_secs=2,
                http_timeout_secs=10,
                latency_sample_count=3,
            )
            result = client.run()
            assert result.latency_ms >= 0
            assert result.jitter_ms >= 0
            assert result.download_mbps >= 0
            assert result.upload_mbps >= 0
            assert result.colo == "MNL"
            assert result.failed_streams >= 0

    def test_cancellation(self) -> None:
        with mock_server() as url:
            client = Client(
                base_url=url,
                parallel_streams=2,
                download_payload_bytes=500_000,
                upload_payload_bytes=500_000,
                measure_duration_secs=10,
                http_timeout_secs=10,
            )
            client.cancel()
            with pytest.raises(CfspeedError):
                client.run_latency()

    def test_proxy_error(self) -> None:
        client = Client(proxy_url="bad-proxy")
        assert client.proxy_error is not None
        assert "missing scheme" in client.proxy_error

    def test_proxy_valid(self) -> None:
        client = Client(proxy_url="http://127.0.0.1:3128")
        assert client.proxy_error is None

    def test_connection_refused(self) -> None:
        client = Client(
            base_url="http://127.0.0.1:1",
            http_timeout_secs=2,
            latency_sample_count=1,
        )
        with pytest.raises(CfspeedError):
            client.run_latency()

    def test_discover_unknown_on_failure(self) -> None:
        """Client defaults to 'unknown' colo/server when discovery endpoint fails."""
        client = Client(
            base_url="http://127.0.0.1:1",
            http_timeout_secs=2,
            latency_sample_count=1,
        )
        # Latency fails first, raising CfspeedError
        with pytest.raises(CfspeedError):
            client.run_latency()
        # Manually verify _discover returns unknown
        colo, server = client._discover()
        assert colo == "unknown"
        assert server == "unknown"
