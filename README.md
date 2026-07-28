# cfspeed

> Cloudflare speed test - Go and Python in one repo.
>
> `go get github.com/techroy23/cfspeed/cfspeed_go` . `pip install cfspeed`

---

## What is this?

**cfspeed** measures your connection to the nearest Cloudflare edge using speed.cloudflare.com. It runs parallel HTTP streams for download and upload, measures idle and loaded latency, and outputs CLI results or JSON.

The repo has two independent implementations:

| Language | Directory | Module / Package | Install |
|----------|-----------|-----------------|---------|
| **Go** | [`cfspeed_go/`](./cfspeed_go) | `github.com/techroy23/cfspeed/cfspeed_go` | `go get` |
| **Python** | [`cfspeed_py/`](./cfspeed_py) | `cfspeed` | `pip install` |

Zero core dependencies in both. SOCKS5 proxy support is optional in Python.

---

## Quick start

### Go

```bash
go get github.com/techroy23/cfspeed/cfspeed_go
```

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/techroy23/cfspeed/cfspeed_go"
)

func main() {
    client := cfspeed.New()
    result, err := client.Run(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(result)
}
```

### Python

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

### CLI usage (both)

```bash
# Go CLI (from the repo)
go run ./cfspeed_go/cmd/cfspeed --parallel 8 --duration 5

# Python CLI (after pip install)
cfspeed -p 8 -d 5

# JSON output
cfspeed --json | jq .download_mbps
```

**Typical flags:**

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

## API reference

### Go

Package `cfspeed` at `github.com/techroy23/cfspeed/cfspeed_go`.

```go
import cfspeed "github.com/techroy23/cfspeed/cfspeed_go"
```

#### `cfspeed.New(opts ...ClientOption) *Client`

Create a client. Options are functional:

```go
client := cfspeed.New(
    cfspeed.WithParallelStreams(8),
    cfspeed.WithMeasureDuration(10 * time.Second),
    cfspeed.WithProxy("http://127.0.0.1:3128"),
)
```

#### `client.Run(ctx) (*Result, error)`

Full speed test (latency -> discovery -> download -> upload).

#### `client.RunDownload(ctx) / client.RunUpload(ctx) / client.RunLatency(ctx)`

Run individual phases.

#### `Result`

```go
type Result struct {
    DownloadMbps          float64
    UploadMbps            float64
    LatencyMs             float64
    JitterMs              float64
    LoadedLatencyMs       float64
    UploadLoadedLatencyMs float64
    Colo                  string
    Server                string
    Timestamp             time.Time
    DownloadBytes         int64
    UploadBytes           int64
    ParallelStreams       int
    FailedStreams         int
}
```

`result.String()` - ASCII table output.
`result.JSON()` - JSON string.
`result.ProxyParseError()` - inspect proxy misconfiguration.

#### `cfspeed.ParseSize(s string) (int64, error)`

Parse human-readable sizes: `"10MB"` -> `10_000_000`, `"1GiB"` -> `1_073_741_824`.

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `WithParallelStreams` | int | 4 | Stream concurrency (clamped 1-64) |
| `WithMeasureDuration` | time.Duration | 10s | Per-phase duration |
| `WithLatencySampleCount` | int | 20 | Latency probe count |
| `WithDownloadPayload` | int | 10 MiB | Download bytes per stream |
| `WithUploadPayload` | int | 10 MiB | Upload bytes per stream |
| `WithHTTPTimeout` | time.Duration | 30s | Request timeout |
| `WithInsecure` | bool | false | Skip TLS verify |
| `WithBaseURL` | string | `https://speed.cloudflare.com` | API endpoint |
| `WithProxy` | string | - | Proxy URL |
| `WithUserAgent` | string | `cfspeed/1.0` | User-Agent header |

---

### Python

Package `cfspeed`.

```python
from cfspeed import Client, Options, Result, parse_size
```

#### `Client(**kwargs)`

Keyword arguments map to `Options` fields.

```python
client = Client(
    parallel_streams=8,
    measure_duration_secs=10.0,
    proxy_url="http://127.0.0.1:3128",
)
```

#### `client.run(timeout=None) -> Result`

Full speed test. Returns a `Result` dataclass, raises `CfspeedError` on failure.

#### `client.run_latency() -> (latency_ms, jitter_ms)`
#### `client.run_download() -> (mbps, loaded_lat_ms, total_bytes, failed_count)`
#### `client.run_upload() -> (mbps, loaded_lat_ms, total_bytes, failed_count)`

#### `Result`

Dataclass with the same fields as the Go version, plus:

```python
result.json()       # -> str (pretty-printed JSON)
str(result)         # -> ASCII table
result.failed_streams  # -> int
```

#### `parse_size(size_str: str) -> int`

Parse `"10MB"` -> `10_000_000`, `"1GiB"` -> `1_073_741_824`. Raises `ValueError` on bad input.

#### `client.proxy_error -> str | None`

Returns a description of proxy misconfiguration, or `None`.

#### `client.cancel()` / `client.is_cancelled`

Cancel an in-flight test. All phases check the cancellation signal before every HTTP call.

#### Options fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `parallel_streams` | int | 4 | Stream concurrency (clamped 1-64) |
| `measure_duration_secs` | float | 10.0 | Per-phase seconds |
| `latency_sample_count` | int | 20 | Latency probes |
| `download_payload_bytes` | int | 10_485_760 | Download bytes/stream |
| `upload_payload_bytes` | int | 10_485_760 | Upload bytes/stream |
| `http_timeout_secs` | float | 30.0 | Request timeout |
| `insecure` | bool | False | Skip TLS verify |
| `base_url` | str | `https://speed.cloudflare.com` | API endpoint |
| `proxy_url` | str | `None` | Proxy (`http://...` or `socks5://...`) |

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

```bash
cfspeed --proxy http://127.0.0.1:3128
```

Works in both Go and Python out of the box.

### SOCKS5

**Go** - built-in, no extra step. Just use `socks5://` URLs:

```go
cfspeed.New(cfspeed.WithProxy("socks5://127.0.0.1:1080"))
```

**Python** - requires the optional `socks5` extra:

```bash
pip install cfspeed[socks5]
cfspeed --proxy socks5://127.0.0.1:1080
```

### Invalid proxy detection

If the proxy URL is malformed (missing scheme, unsupported protocol), the client shows it:

```go
if err := client.ProxyParseError(); err != nil {
    log.Printf("warning: proxy ignored: %v", err)
}
```

```python
if client.proxy_error:
    print(f"warning: {client.proxy_error}")
```

---

## Testing

### Go

```bash
cd cfspeed_go
go test -v -count=1 -short ./...          # 35 tests
go test -race -count=1 -short ./...       # race detector
```

### Python

```bash
cd cfspeed_py
pip install -e .[socks5]
pip install pytest
pytest tests/ -v                          # 26 tests
```

---

## Requirements

- **Go >= 1.22** (zero external dependencies)
- **Python >= 3.10** (zero external dependencies; PySocks optional for SOCKS5)
- Both implementations use the same measurement algorithm and output format
- Go module path: `github.com/techroy23/cfspeed/cfspeed_go`

---

## FAQ

**Q: Why are there two implementations?**
A: Go for CLI distribution via `go install`. Python for scripting and automation.

**Q: Do they share code?**
A: No - each is an independent port with the same behaviour.

**Q: Can I use this behind a NAT or CGNAT?**
A: Yes. Cloudflare's test uses short-lived HTTP connections that work behind NAT. Your external IP is never sent to us.

**Q: What does loaded latency mean?**
A: Latency measured while the connection is under download/upload load. The gap between idle and loaded latency tells you how bad your bufferbloat is.

**Q: Do I need a Cloudflare account?**
A: No. The test hits public endpoints at speed.cloudflare.com.

---

## License

MIT (c) 2026 techroy23
