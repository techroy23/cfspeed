"""cfspeed CLI — ``cfspeed [options]``"""

from __future__ import annotations

import argparse
import signal
import sys
import time

from cfspeed import Client, parse_size
from cfspeed.client import CfspeedError, DEFAULT_BASE_URL


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cfspeed",
        description="Cloudflare speed test CLI — port of golang cfspeed",
    )
    p.add_argument(
        "--parallel", "-p",
        type=int,
        default=4,
        help="Number of parallel streams (1–64, default: 4)",
    )
    p.add_argument(
        "--duration", "-d",
        type=float,
        default=10.0,
        help="Measurement duration per phase in seconds (default: 10)",
    )
    p.add_argument(
        "--download-size", "-ds",
        type=str,
        default="10MiB",
        help="Download payload per stream (e.g. 10MiB, 5MB, default: 10MiB)",
    )
    p.add_argument(
        "--upload-size", "-us",
        type=str,
        default="10MiB",
        help="Upload payload per stream (e.g. 10MiB, 5MB, default: 10MiB)",
    )
    p.add_argument(
        "--latency-samples", "-l",
        type=int,
        default=20,
        help="Number of latency probe samples (default: 20)",
    )
    p.add_argument(
        "--http-timeout", "-t",
        type=float,
        default=30.0,
        help="Per-request HTTP timeout in seconds (default: 30)",
    )
    p.add_argument(
        "--insecure", "-k",
        action="store_true",
        help="Skip TLS certificate verification",
    )
    p.add_argument(
        "--base-url", "-b",
        type=str,
        default=DEFAULT_BASE_URL,
        help=f"Speed test base URL (default: {DEFAULT_BASE_URL})",
    )
    p.add_argument(
        "--proxy", "-x",
        type=str,
        default=None,
        help="HTTP/HTTPS/SOCKS5 proxy URL (e.g. http://127.0.0.1:3128)",
    )
    p.add_argument(
        "--json", "-j",
        action="store_true",
        help="Output result as JSON",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Overall test timeout in seconds (default: no limit)",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    # Parse sizes
    try:
        down_bytes = parse_size(args.download_size)
        up_bytes = parse_size(args.upload_size)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    # Build client
    client = Client(
        parallel_streams=args.parallel,
        measure_duration_secs=args.duration,
        download_payload_bytes=down_bytes,
        upload_payload_bytes=up_bytes,
        latency_sample_count=args.latency_samples,
        http_timeout_secs=args.http_timeout,
        insecure=args.insecure,
        base_url=args.base_url,
        proxy_url=args.proxy,
    )

    # Check proxy error
    if client.proxy_error:
        print(f"warning: {client.proxy_error}", file=sys.stderr)

    # Set up signal handling for clean cancellation
    def _on_sigint(signum: int, frame: object) -> None:
        client.cancel()
        print("\nInterrupted — shutting down...", file=sys.stderr)

    original_sigint = signal.signal(signal.SIGINT, _on_sigint)

    # Run
    try:
        t0 = time.perf_counter()
        result = client.run(timeout=args.timeout)
        elapsed = time.perf_counter() - t0

        if client.is_cancelled:
            print("Test cancelled.", file=sys.stderr)
            return 1

        if args.json:
            print(result.json())
        else:
            print(result)
            print(f"\nTest completed in {elapsed:.1f}s")
    except CfspeedError as e:
        if client.is_cancelled:
            print("Test cancelled.", file=sys.stderr)
            return 1
        print(f"error: {e}", file=sys.stderr)
        return 1
    finally:
        signal.signal(signal.SIGINT, original_sigint)

    return 0


if __name__ == "__main__":
    sys.exit(main())
