// Package cfspeed measures internet speed using Cloudflare's global
// speed test infrastructure (speed.cloudflare.com). Zero external deps.
//
// Basic usage:
//
//	client := cfspeed.New()
//	result, err := client.Run(context.Background())
//
// With options:
//
//	client := cfspeed.New(
//	    cfspeed.WithParallelStreams(8),
//	    cfspeed.WithMeasureDuration(15 * time.Second),
//	)
//
// The package exposes a clean, idiomatic Go API with functional options,
// context support, and structured results suitable for use as a library
// in any Go project or as a standalone CLI tool.
package cfspeed

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// maxParallel is the safety ceiling for concurrent streams.
	maxParallel = 64
	// userAgent sent with all requests.
	userAgent = "cfspeed/1.0"
)

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

// Result holds the complete speed test outcome.
type Result struct {
	// Download speed in Megabits per second.
	DownloadMbps float64 `json:"download_mbps"`
	// Upload speed in Megabits per second.
	UploadMbps float64 `json:"upload_mbps"`
	// Idle latency (round-trip time) in milliseconds.
	LatencyMs float64 `json:"latency_ms"`
	// Jitter in milliseconds (mean absolute deviation of latency samples).
	JitterMs float64 `json:"jitter_ms"`
	// Loaded latency during download, in milliseconds (or 0 if not measured).
	LoadedLatencyMs float64 `json:"loaded_latency_ms,omitempty"`
	// Loaded latency during upload, in milliseconds (or 0 if not measured).
	UploadLoadedLatencyMs float64 `json:"upload_loaded_latency_ms,omitempty"`
	// Data center / PoP code returned by Cloudflare (e.g. "MNL", "NRT").
	Colo string `json:"colo,omitempty"`
	// Server location string (e.g. "Asia Pacific (MNL)").
	Server string `json:"server,omitempty"`
	// Test timestamp.
	Timestamp time.Time `json:"timestamp"`
	// Actual bytes received during download test.
	DownloadBytes int64 `json:"download_bytes"`
	// Actual bytes sent during upload test.
	UploadBytes int64 `json:"upload_bytes"`
	// Number of parallel streams used.
	ParallelStreams int `json:"parallel_streams"`
	// Number of streams that failed during the test (partial failure).
	FailedStreams int `json:"failed_streams,omitempty"`
}

// String returns a human-readable summary.
func (r Result) String() string {
	var loadedLatStr string
	if r.UploadLoadedLatencyMs > 0 {
		loadedLatStr = fmt.Sprintf("%.2f / %.2f", r.LoadedLatencyMs, r.UploadLoadedLatencyMs)
	} else {
		loadedLatStr = fmt.Sprintf("%.2f", r.LoadedLatencyMs)
	}

	var failedStr string
	if r.FailedStreams > 0 {
		failedStr = fmt.Sprintf(" (%d failed)", r.FailedStreams)
	}

	return fmt.Sprintf(
		"╔══════════════════════════════════╗\n"+
			"║  Cloudflare Speed Test Result     ║\n"+
			"╠══════════════════════════════════╣\n"+
			"║  Download:    %8.2f Mbps      ║\n"+
			"║  Upload:      %8.2f Mbps      ║\n"+
			"║  Latency:     %8.2f ms        ║\n"+
			"║  Jitter:      %8.2f ms        ║\n"+
			"║  Loaded Lat:  %-14s      ║\n"+
			"║  Server:      %-18s ║\n"+
			"║  Colo:        %-18s ║\n"+
			"║  Data:        %s%s         ║\n"+
			"╚══════════════════════════════════╝",
		r.DownloadMbps,
		r.UploadMbps,
		r.LatencyMs,
		r.JitterMs,
		loadedLatStr,
		r.Server,
		r.Colo,
		formatBytes(r.DownloadBytes+r.UploadBytes),
		failedStr,
	)
}

// JSON marshals the result as indented JSON.
func (r Result) JSON() string {
	b, _ := json.MarshalIndent(r, "", "  ")
	return string(b)
}

// ClientOption configures the speed test client.
type ClientOption func(*clientOpts)

// clientOpts holds all configurable parameters.
type clientOpts struct {
	downloadBytes  int64
	uploadBytes    int64
	parallel       int
	latencySamples int
	measureDur     time.Duration
	httpTimeout    time.Duration
	insecure       bool
	proxyURL       string
	baseURL        string
}

// ---------------------------------------------------------------------------
// Client options (functional options pattern)
// ---------------------------------------------------------------------------

// WithDownloadPayload sets the download payload per stream in bytes (default 10MB).
func WithDownloadPayload(bytes int64) ClientOption {
	return func(o *clientOpts) { o.downloadBytes = bytes }
}

// WithUploadPayload sets the upload payload per stream in bytes (default 10MB).
func WithUploadPayload(bytes int64) ClientOption {
	return func(o *clientOpts) { o.uploadBytes = bytes }
}

// WithParallelStreams sets the number of concurrent streams (default 4, max 64).
func WithParallelStreams(n int) ClientOption {
	return func(o *clientOpts) {
		if n < 1 {
			n = 1
		}
		if n > maxParallel {
			n = maxParallel
		}
		o.parallel = n
	}
}

// WithLatencySamples sets how many latency probes to send (default 5).
func WithLatencySamples(n int) ClientOption {
	return func(o *clientOpts) { o.latencySamples = n }
}

// WithMeasureDuration sets the per-test measurement window (default 10s).
func WithMeasureDuration(d time.Duration) ClientOption {
	return func(o *clientOpts) { o.measureDur = d }
}

// WithHTTPTimeout sets the HTTP client timeout (default 30s).
func WithHTTPTimeout(d time.Duration) ClientOption {
	return func(o *clientOpts) { o.httpTimeout = d }
}

// WithInsecure disables TLS certificate verification (for testing only).
func WithInsecure() ClientOption {
	return func(o *clientOpts) { o.insecure = true }
}

// WithProxy sets an HTTP/SOCKS5 proxy URL.
// Supported schemes: http, https, socks5.
func WithProxy(proxyURL string) ClientOption {
	return func(o *clientOpts) { o.proxyURL = proxyURL }
}

// WithBaseURL overrides the speed test endpoint base (for testing).
func WithBaseURL(base string) ClientOption {
	return func(o *clientOpts) { o.baseURL = base }
}

// ---------------------------------------------------------------------------
// Default options
// ---------------------------------------------------------------------------

func defaultOpts() clientOpts {
	return clientOpts{
		downloadBytes:  10 * 1_000_000, // 10 MB per stream
		uploadBytes:    10 * 1_000_000, // 10 MB per stream
		parallel:       4,
		latencySamples: 5,
		measureDur:     10 * time.Second,
		httpTimeout:    30 * time.Second,
		baseURL:        "https://speed.cloudflare.com",
	}
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

// Client is the speed test client.
type Client struct {
	opts          clientOpts
	httpCli       *http.Client
	proxyParseErr error // set when WithProxy URL is malformed
}

// ProxyParseError returns any proxy URL parse error encountered during
// construction, or nil if the proxy URL was valid or no proxy was configured.
func (c *Client) ProxyParseError() error {
	return c.proxyParseErr
}

// New creates a new Client with the given options.
func New(opts ...ClientOption) *Client {
	o := defaultOpts()
	for _, fn := range opts {
		fn(&o)
	}

	transport := &http.Transport{
		MaxIdleConns:        o.parallel * 4,
		MaxConnsPerHost:     o.parallel * 4,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
	}

	c := &Client{
		opts: o,
		httpCli: &http.Client{
			Timeout:   o.httpTimeout,
			Transport: transport,
		},
	}

	if o.proxyURL != "" {
		proxyURL, err := url.Parse(o.proxyURL)
		if err != nil {
			c.proxyParseErr = fmt.Errorf("proxy URL %q: %w", o.proxyURL, err)
		} else if proxyURL.Scheme == "" {
			c.proxyParseErr = fmt.Errorf("proxy URL %q: missing scheme (use http://, https://, or socks5://)", o.proxyURL)
		} else {
			transport.Proxy = http.ProxyURL(proxyURL)
		}
	}

	if o.insecure {
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true, // #nosec — opt-in
		}
	}

	return c
}

// doReq creates and executes an HTTP request, setting the User-Agent header.
func (c *Client) doReq(ctx context.Context, method, url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	return c.httpCli.Do(req)
}

// ---------------------------------------------------------------------------
// Public measurement methods
// ---------------------------------------------------------------------------

// Run executes a full speed test in order: latency → download → upload.
// It respects context cancellation for clean shutdown.
func (c *Client) Run(ctx context.Context) (*Result, error) {
	res := &Result{
		Timestamp:       time.Now().UTC(),
		ParallelStreams: c.opts.parallel,
	}

	// 1. Discover colo / server info (non-fatal if fails).
	_ = c.discover(ctx, res)
	if res.Server == "" {
		res.Server = "unknown"
	}

	// 2. Idle latency + jitter.
	latencies, err := c.measureLatency(ctx)
	if err != nil {
		return nil, fmt.Errorf("latency test: %w", err)
	}
	res.LatencyMs = median(latencies)
	res.JitterMs = mad(latencies)

	if ctx.Err() != nil {
		return res, ctx.Err()
	}

	// 3. Download test.
	downMbps, loadedLat, downBytes, downFailures, err := c.measureDownload(ctx)
	if err != nil {
		return nil, fmt.Errorf("download test: %w", err)
	}
	res.DownloadMbps = downMbps
	res.DownloadBytes = downBytes
	res.LoadedLatencyMs = loadedLat
	res.FailedStreams += downFailures

	if ctx.Err() != nil {
		return res, ctx.Err()
	}

	// 4. Upload test.
	upMbps, upLoadedLat, upBytes, upFailures, err := c.measureUpload(ctx)
	if err != nil {
		return nil, fmt.Errorf("upload test: %w", err)
	}
	res.UploadMbps = upMbps
	res.UploadBytes = upBytes
	res.UploadLoadedLatencyMs = upLoadedLat
	res.FailedStreams += upFailures

	return res, nil
}

// RunDownload runs only the download test.
func (c *Client) RunDownload(ctx context.Context) (mbps float64, loadedLatMs float64, bytes int64, err error) {
	mbps, loadedLatMs, bytes, _, err = c.measureDownload(ctx)
	return
}

// RunUpload runs only the upload test.
func (c *Client) RunUpload(ctx context.Context) (mbps float64, loadedLatMs float64, bytes int64, err error) {
	mbps, loadedLatMs, bytes, _, err = c.measureUpload(ctx)
	return
}

// RunLatency runs only the latency test.
func (c *Client) RunLatency(ctx context.Context) (latencyMs, jitterMs float64, err error) {
	latencies, err := c.measureLatency(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("latency test: %w", err)
	}
	return median(latencies), mad(latencies), nil
}

// ---------------------------------------------------------------------------
// Internal: discovery — uses Cloudflare's standard /cdn-cgi/trace endpoint
// which returns key=value pairs about the current edge connection.
// ---------------------------------------------------------------------------

func (c *Client) discover(ctx context.Context, res *Result) error {
	resp, err := c.doReq(ctx, http.MethodGet, c.opts.baseURL+"/cdn-cgi/trace", nil)
	if err != nil {
		resp, err = c.doReq(ctx, http.MethodGet, "https://1.1.1.1/cdn-cgi/trace", nil)
		if err != nil {
			return err
		}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	lines := bytes.Split(bytes.TrimSpace(body), []byte("\n"))
	for _, line := range lines {
		parts := bytes.SplitN(line, []byte("="), 2)
		if len(parts) != 2 {
			continue
		}
		key := string(bytes.TrimSpace(parts[0]))
		val := string(bytes.TrimSpace(parts[1]))
		switch key {
		case "colo":
			res.Colo = val
		case "loc":
			if res.Server == "" {
				res.Server = val
			}
		}
	}

	if res.Server == "" {
		res.Server = "unknown"
	}
	return nil
}

// ---------------------------------------------------------------------------
// Internal: latency
// ---------------------------------------------------------------------------

func (c *Client) measureLatency(ctx context.Context) ([]float64, error) {
	var (
		samples  []float64
		lastErr  error
	)

	for i := 0; i < c.opts.latencySamples; i++ {
		select {
		case <-ctx.Done():
			return samples, ctx.Err()
		default:
		}

		start := time.Now()
		resp, err := c.doReq(ctx, http.MethodHead,
			c.opts.baseURL+"/__down?bytes=0", nil)
		if err != nil {
			lastErr = err
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		samples = append(samples, float64(time.Since(start).Microseconds())/1000.0)
	}

	if len(samples) == 0 {
		if lastErr != nil {
			return nil, fmt.Errorf("all latency probes failed: %w", lastErr)
		}
		return nil, fmt.Errorf("all latency probes failed")
	}

	return samples, nil
}

// ---------------------------------------------------------------------------
// Internal: download
// ---------------------------------------------------------------------------

func (c *Client) measureDownload(ctx context.Context) (mbps float64, loadedLat float64, bytes int64, failedCount int, err error) {
	var (
		mu       sync.Mutex
		total    int64
		errs     []error
		latMu    sync.Mutex
		latSamps []float64
		wg       sync.WaitGroup
		failures int32
	)

	ctxLat, cancelLat := context.WithCancel(ctx)
	defer cancelLat()

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctxLat.Done():
				return
			case <-ticker.C:
				start := time.Now()
				resp, err := c.doReq(ctxLat, http.MethodHead,
					c.opts.baseURL+"/__down?bytes=0", nil)
				if err != nil {
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				latMu.Lock()
				latSamps = append(latSamps, float64(time.Since(start).Microseconds())/1000.0)
				latMu.Unlock()
			}
		}
	}()

	start := time.Now()
	deadline := start.Add(c.opts.measureDur)
	ctxDl, cancelDl := context.WithDeadline(ctx, deadline)

	for i := 0; i < c.opts.parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, e := c.downloadStream(ctxDl, deadline)
			mu.Lock()
			total += n
			if e != nil {
				errs = append(errs, e)
				failures++
			}
			mu.Unlock()
		}()
	}

	wg.Wait()
	cancelDl()
	elapsed := time.Since(start).Seconds()

	if total == 0 && len(errs) > 0 {
		return 0, 0, 0, int(failures), errs[0]
	}
	if total == 0 {
		return 0, 0, 0, int(failures), fmt.Errorf("download test: zero bytes transferred")
	}

	mbps = float64(total*8) / 1_000_000 / elapsed

	latMu.Lock()
	if len(latSamps) > 0 {
		loadedLat = median(latSamps)
	}
	latMu.Unlock()

	return mbps, loadedLat, total, int(failures), nil
}

func (c *Client) downloadStream(ctx context.Context, deadline time.Time) (int64, error) {
	var total int64
	for {
		if time.Now().After(deadline) {
			break
		}
		if ctx.Err() != nil {
			break
		}

		u := fmt.Sprintf("%s/__down?bytes=%d&nocache=%d",
			c.opts.baseURL, c.opts.downloadBytes, time.Now().UnixNano())
		resp, err := c.doReq(ctx, http.MethodGet, u, nil)
		if err != nil {
			return total, err
		}
		n, _ := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		total += n
	}
	return total, nil
}

// ---------------------------------------------------------------------------
// Internal: upload
// ---------------------------------------------------------------------------

func (c *Client) measureUpload(ctx context.Context) (mbps float64, loadedLat float64, bytes int64, failedCount int, err error) {
	var (
		mu       sync.Mutex
		total    int64
		errs     []error
		latMu    sync.Mutex
		latSamps []float64
		wg       sync.WaitGroup
		failures int32
	)

	ctxLat, cancelLat := context.WithCancel(ctx)
	defer cancelLat()

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctxLat.Done():
				return
			case <-ticker.C:
				start := time.Now()
				resp, err := c.doReq(ctxLat, http.MethodHead,
					c.opts.baseURL+"/__down?bytes=0", nil)
				if err != nil {
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				latMu.Lock()
				latSamps = append(latSamps, float64(time.Since(start).Microseconds())/1000.0)
				latMu.Unlock()
			}
		}
	}()

	start := time.Now()
	deadline := start.Add(c.opts.measureDur)
	ctxDl, cancelDl := context.WithDeadline(ctx, deadline)

	for i := 0; i < c.opts.parallel; i++ {
		// Generate random payload per goroutine (lazy allocation).
		// Each goroutine gets its own buffer so uploadStream can send it repeatedly
		// without data races on the buffer contents.
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := make([]byte, c.opts.uploadBytes)
			if _, err := rand.Read(payload); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("random payload gen: %w", err))
				failures++
				mu.Unlock()
				return
			}
			n, e := c.uploadStream(ctxDl, deadline, payload)
			mu.Lock()
			total += n
			if e != nil {
				errs = append(errs, e)
				failures++
			}
			mu.Unlock()
		}()
	}

	wg.Wait()
	cancelDl()
	elapsed := time.Since(start).Seconds()

	if total == 0 && len(errs) > 0 {
		return 0, 0, 0, int(failures), errs[0]
	}
	if total == 0 {
		return 0, 0, 0, int(failures), fmt.Errorf("upload test: zero bytes transferred")
	}

	mbps = float64(total*8) / 1_000_000 / elapsed

	latMu.Lock()
	if len(latSamps) > 0 {
		loadedLat = median(latSamps)
	}
	latMu.Unlock()

	return mbps, loadedLat, total, int(failures), nil
}

func (c *Client) uploadStream(ctx context.Context, deadline time.Time, payload []byte) (int64, error) {
	var total int64
	for {
		if time.Now().After(deadline) {
			break
		}
		if ctx.Err() != nil {
			break
		}

		body := bytes.NewReader(payload)
		resp, err := c.doReq(ctx, http.MethodPost,
			c.opts.baseURL+"/__up?nocache="+fmt.Sprint(time.Now().UnixNano()),
			body)
		if err != nil {
			return total, err
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		total += int64(len(payload))
	}
	return total, nil
}

// ---------------------------------------------------------------------------
// Stats helpers
// ---------------------------------------------------------------------------

// median returns the median of a float64 slice.
func median(samples []float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := make([]float64, len(samples))
	copy(sorted, samples)
	sort.Float64s(sorted)

	n := len(sorted)
	if n%2 == 0 {
		return (sorted[n/2-1] + sorted[n/2]) / 2
	}
	return sorted[n/2]
}

// mad returns the mean absolute deviation from the median (a measure of jitter).
func mad(samples []float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	m := median(samples)
	var sum float64
	for _, v := range samples {
		sum += math.Abs(v - m)
	}
	return sum / float64(len(samples))
}

// formatBytes returns a human-readable byte string using decimal (1000-based) units
// to remain consistent with the Mbps (1,000,000 bits per Megabit) convention.
func formatBytes(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// ParseSize converts a human-readable size string to bytes.
// Supports: MB (1,000,000), MiB (1,048,576), GB (1,000,000,000),
// GiB (1,073,741,824), KB (1,000), KiB (1,024), B (1).
// Case-insensitive. e.g. "10MB", "1GiB", "500mib", "2gb".
func ParseSize(s string) (int64, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	if len(s) < 2 {
		return 0, fmt.Errorf("invalid size: %q (use e.g. 10MB, 1GB, 500MiB)", s)
	}

	var multiplier int64
	switch {
	case strings.HasSuffix(s, "GIB"):
		multiplier = 1_073_741_824
		s = s[:len(s)-3]
	case strings.HasSuffix(s, "GB"):
		multiplier = 1_000_000_000
		s = s[:len(s)-2]
	case strings.HasSuffix(s, "MIB"):
		multiplier = 1_048_576
		s = s[:len(s)-3]
	case strings.HasSuffix(s, "MB"):
		multiplier = 1_000_000
		s = s[:len(s)-2]
	case strings.HasSuffix(s, "KIB"):
		multiplier = 1_024
		s = s[:len(s)-3]
	case strings.HasSuffix(s, "KB"):
		multiplier = 1_000
		s = s[:len(s)-2]
	case strings.HasSuffix(s, "B"):
		multiplier = 1
		s = s[:len(s)-1]
	default:
		return 0, fmt.Errorf("unknown unit in %q (use MB, MiB, GB, GiB, KB, KiB, or B)", s)
	}

	var val int64
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse number in %q: %w", s, err)
	}

	return val * multiplier, nil
}
