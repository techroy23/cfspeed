# cfspeed

> Cloudflare speed test library for Python.
>
> `pip install cfspeed`

---

## What is this?

**cfspeed** measures your connection to the nearest Cloudflare edge using speed.cloudflare.com. It runs parallel HTTP streams for download and upload, measures idle and loaded latency, and outputs CLI results or JSON.

Zero core dependencies. SOCKS5 proxy support is optional.

---

## Quick start

```bash
pip install cfspeed
```

```python
import cfspeed

client = cfspeed.Client(parallel_streams=4)
result = client.run(timeout=30)
print(result)
print(result.json())
```

### CLI

```bash
# Default run
cfspeed

# With custom options
cfspeed --parallel 8 --duration 5 --json

# JSON output piped to jq
cfspeed --json | jq .download_mbps

# Behind a proxy
cfspeed --proxy http://127.0.0.1:3128
cfspeed --proxy socks5://127.0.0.1:1080  # requires pip install cfspeed[socks5]
```

**CLI flags:**

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--parallel` | `-p` | 4 | Number of parallel streams (1-64) |
| `--duration` | `-d` | 10 | Measurement duration per phase (seconds) |
| `--download-size` | `-ds` | 10MiB | Bytes per download stream |
| `--upload-size` | `-us` | 10MiB | Bytes per upload stream |
| `--http-timeout` | `-t` | 30 | Per-request timeout (seconds) |
| `--proxy` | `-x` | - | Proxy URL (`http://...`, `socks5://...`) |
| `--insecure` | `-k` | false | Skip TLS verification |
| `--json` | `-j` | false | Output as JSON |

---

## Python API

```python
from cfspeed import Client, Result, parse_size
```

### `Client(**kwargs)`

Keyword arguments map directly to configuration:

```python
client = Client(
    parallel_streams=8,       # Stream concurrency (clamped 1-64)
    measure_duration_secs=10.0, # Per-phase duration
    latency_sample_count=20,  # Latency probe count
    download_payload_bytes=10_485_760,  # 10 MiB per stream
    upload_payload_bytes=10_485_760,    # 10 MiB per stream
    http_timeout_secs=30.0,   # Request timeout
    insecure=False,           # Skip TLS verification
    base_url="https://speed.cloudflare.com",
    proxy_url=None,           # http:// or socks5:// URL
)
```

### `client.run(timeout=None) -> Result`

Full speed test - runs latency, discovery, download, and upload in sequence. Raises `CfspeedError` on failure.

```python
result = client.run()
```

### Individual phases

```python
latency_ms, jitter_ms = client.run_latency()
mbps, loaded_lat_ms, total_bytes, failed_count = client.run_download()
mbps, loaded_lat_ms, total_bytes, failed_count = client.run_upload()
```

### `Result`

A dataclass with all measurement fields:

```python
result.download_mbps          # float
result.upload_mbps            # float
result.latency_ms             # float
result.jitter_ms              # float
result.loaded_latency_ms      # float - latency under download load
result.upload_loaded_latency_ms  # float - latency under upload load
result.colo                   # str - Cloudflare PoP code (e.g. "MNL")
result.server                 # str - server location
result.timestamp              # datetime (UTC)
result.download_bytes         # int
result.upload_bytes           # int
result.parallel_streams       # int
result.failed_streams         # int - streams that errored out

str(result)                   # ASCII table output
result.json()                 # pretty-printed JSON string
```

### `parse_size(size_str: str) -> int`

Parse human-readable sizes. Raises `ValueError` on bad input.

```python
parse_size("10MB")    # -> 10_000_000
parse_size("1GiB")    # -> 1_073_741_824
parse_size("500KB")   # -> 500_000
parse_size("100 B")   # -> 100
```

### Cancellation

```python
client.cancel()
client.is_cancelled   # -> bool
```

Call `cancel()` from another thread or a signal handler (SIGINT) to stop an in-flight test. All phases check the flag before every HTTP call.

### Proxy error detection

```python
if client.proxy_error:
    print(f"warning: {client.proxy_error}")
```

Returns a description of proxy misconfiguration (missing scheme, unsupported protocol), or `None`.

---

## How it works

```
1. Latency   -> HEAD /__down?bytes=0       (x20 samples)   -> median + MAD
2. Discovery -> GET  /cdn-cgi/trace        (1 request)      -> colo + location
3. Download  -> GET  /__down?bytes=N       (parallel loop)  -> Mbps + loaded latency
4. Upload    -> POST /__up                 (parallel loop)  -> Mbps + loaded latency
```

Download and upload run background HEAD probes every 500ms to measure loaded latency - how your connection behaves under traffic.

### Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `speed.cloudflare.com/__down?bytes=N` | GET | Download N bytes of PRNG data |
| `speed.cloudflare.com/__up` | POST | Upload random data |
| `speed.cloudflare.com/cdn-cgi/trace` | GET | Identify colo (PoP) and location |
| `speed.cloudflare.com/__down?bytes=0` | HEAD | Lightweight latency probe |

---

## Output examples

### CLI

```
+-------------------------------------+
|  Cloudflare Speed Test Result       |
+-------------------------------------+
|  Download:            24,545 Mbps   |
|  Upload:              24,649 Mbps   |
|  Latency:                0.04 ms    |
|  Jitter:                 0.01 ms    |
|  Loaded Lat:        0.16 / 0.29     |
|  Server:      MNL                   |
|  Colo:        MNL                   |
|  Data:        19.5 GB (2 failed)    |
+-------------------------------------+
```

### JSON

```json
{
  "download_mbps": 24545.0,
  "upload_mbps": 24649.0,
  "latency_ms": 0.04,
  "jitter_ms": 0.01,
  "loaded_latency_ms": 0.16,
  "upload_loaded_latency_ms": 0.29,
  "colo": "MNL",
  "server": "MNL",
  "timestamp": "2026-07-28T14:51:46Z",
  "download_bytes": 4227486720,
  "upload_bytes": 5497500000,
  "parallel_streams": 4,
  "failed_streams": 2
}
```

---

## Proxy support

### HTTP / HTTPS

Works out of the box:

```bash
cfspeed --proxy http://127.0.0.1:3128
```

```python
Client(proxy_url="http://127.0.0.1:3128")
```

### SOCKS5

Requires the optional `socks5` extra:

```bash
pip install cfspeed[socks5]
cfspeed --proxy socks5://127.0.0.1:1080
```

```python
Client(proxy_url="socks5://127.0.0.1:1080")
```

### Invalid proxy detection

If the proxy URL is malformed, the client shows a warning:

```python
if client.proxy_error:
    print(f"warning: {client.proxy_error}")
```

---

## Testing

```bash
pip install pytest
pytest tests/ -v    # 26 tests
```

---

## Requirements

- **Python >= 3.10**
- Zero required dependencies
- Optional: `PySocks>=1.7.1` for SOCKS5 proxy support

---

## FAQ

**Q: What does loaded latency mean?**
A: Latency measured while the connection is under download/upload load. The gap between idle and loaded latency tells you how bad your bufferbloat is.

**Q: Can I use this behind a NAT or CGNAT?**
A: Yes. Cloudflare's test uses short-lived HTTP connections that work behind NAT.

**Q: Do I need a Cloudflare account?**
A: No. The test hits public endpoints at speed.cloudflare.com.

**Q: How is this different from speedtest-cli?**
A: cfspeed uses Cloudflare's own speed test infrastructure (same one at speed.cloudflare.com), runs parallel HTTP streams, and measures loaded latency.

---

## License

MIT (c) 2026 techroy23
