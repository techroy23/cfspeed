package cfspeed

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// Unit tests (mock server, no network)
// =============================================================================

// testServer is a minimal mock of speed.cloudflare.com for unit testing.
func newTestServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cdn-cgi/trace":
			w.Write([]byte("colo=MNL\nloc=MNL"))
		case "/__down":
			bytesStr := r.URL.Query().Get("bytes")
			if bytesStr == "0" {
				w.WriteHeader(http.StatusOK)
				return
			}
			var size int
			fmt.Sscanf(bytesStr, "%d", &size)
			if size <= 0 {
				size = 1_000_000
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
			w.WriteHeader(http.StatusOK)
			chunk := make([]byte, 65536)
			for written := 0; written < size; written += len(chunk) {
				left := size - written
				if left < len(chunk) {
					w.Write(chunk[:left])
					break
				}
				w.Write(chunk)
			}
		case "/__up":
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestLatency(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithLatencySamples(3),
		WithHTTPTimeout(5*time.Second),
	)

	latency, jitter, err := client.RunLatency(context.Background())
	if err != nil {
		t.Fatalf("RunLatency failed: %v", err)
	}
	if latency < 0 {
		t.Errorf("negative latency: %f", latency)
	}
	if jitter < 0 {
		t.Errorf("negative jitter: %f", jitter)
	}
	t.Logf("Latency: %.2f ms, Jitter: %.2f ms", latency, jitter)
}

func TestDownload(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(2),
		WithDownloadPayload(1_000_000),
		WithMeasureDuration(3*time.Second),
		WithHTTPTimeout(10*time.Second),
	)

	mbps, loadedLat, bytes, err := client.RunDownload(context.Background())
	if err != nil {
		t.Fatalf("RunDownload failed: %v", err)
	}
	if mbps <= 0 {
		t.Errorf("download speed should be > 0, got %f", mbps)
	}
	if bytes <= 0 {
		t.Errorf("download bytes should be > 0, got %d", bytes)
	}
	t.Logf("Download: %.2f Mbps, Loaded Lat: %.2f ms, Bytes: %d", mbps, loadedLat, bytes)
}

func TestUpload(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(2),
		WithUploadPayload(1_000_000),
		WithMeasureDuration(3*time.Second),
		WithHTTPTimeout(10*time.Second),
	)

	mbps, loadedLat, bytes, err := client.RunUpload(context.Background())
	if err != nil {
		t.Fatalf("RunUpload failed: %v", err)
	}
	if mbps <= 0 {
		t.Errorf("upload speed should be > 0, got %f", mbps)
	}
	if bytes <= 0 {
		t.Errorf("upload bytes should be > 0, got %d", bytes)
	}
	t.Logf("Upload: %.2f Mbps, Loaded Lat: %.2f ms, Bytes: %d", mbps, loadedLat, bytes)
}

func TestFullRun(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(2),
		WithDownloadPayload(1_000_000),
		WithUploadPayload(1_000_000),
		WithMeasureDuration(3*time.Second),
		WithHTTPTimeout(15*time.Second),
	)

	res, err := client.Run(context.Background())
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if res.Colo != "MNL" {
		t.Errorf("expected colo MNL, got %s", res.Colo)
	}
	if res.DownloadMbps <= 0 {
		t.Errorf("download > 0, got %f", res.DownloadMbps)
	}
	if res.UploadMbps <= 0 {
		t.Errorf("upload > 0, got %f", res.UploadMbps)
	}
	if res.LatencyMs <= 0 {
		t.Errorf("latency > 0, got %f", res.LatencyMs)
	}
	if res.DownloadBytes <= 0 {
		t.Errorf("download bytes > 0, got %d", res.DownloadBytes)
	}
	if res.UploadBytes <= 0 {
		t.Errorf("upload bytes > 0, got %d", res.UploadBytes)
	}

	t.Logf("Result:\n%s", res.String())
}

// =============================================================================
// Edge case: Context cancellation
// =============================================================================

func TestContextCancellationBeforeRun(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(4),
		WithDownloadPayload(250_000_000),
		WithMeasureDuration(60*time.Second),
		WithHTTPTimeout(5*time.Second),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := client.Run(ctx)
	if err == nil {
		t.Fatal("expected error due to context cancellation, got nil")
	}
	t.Logf("Got expected cancellation error: %v", err)
}

func TestContextCancellationMidDownload(t *testing.T) {
	done := make(chan struct{})

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cdn-cgi/trace":
			w.Write([]byte("colo=MNL\nloc=MNL"))
		case "/__down":
			w.WriteHeader(http.StatusOK)
			chunk := make([]byte, 65536)
			flusher, canFlush := w.(http.Flusher)
			for {
				select {
				case <-done:
					return
				case <-r.Context().Done():
					return
				default:
				}
				_, err := w.Write(chunk)
				if err != nil {
					return
				}
				if canFlush {
					flusher.Flush()
				}
			}
		case "/__up":
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	srv.Config.WriteTimeout = 5 * time.Second
	srv.Start()
	defer func() {
		close(done)
		srv.Close()
	}()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(2),
		WithDownloadPayload(1_000_000_000), // 1GB — will never complete
		WithMeasureDuration(60*time.Second),
		WithHTTPTimeout(5*time.Second),
		WithLatencySamples(3),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	_, err := client.Run(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from context cancellation, got nil")
	}
	if elapsed > 15*time.Second {
		t.Errorf("cancellation was too slow: took %v", elapsed)
	}
	t.Logf("Cancellation within %v: %v", elapsed, err)
}

// =============================================================================
// Edge case: Network failures
// =============================================================================

func TestServerConnectionRefused(t *testing.T) {
	client := New(
		WithBaseURL("http://127.0.0.1:1"),
		WithHTTPTimeout(3*time.Second),
	)

	_, err := client.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for connection refused, got nil")
	}
	t.Logf("Got expected connection error: %v", err)
}

func TestDNSError(t *testing.T) {
	client := New(
		WithBaseURL("http://this-domain-definitely-does-not-exist-123456789.com"),
		WithHTTPTimeout(3*time.Second),
	)

	_, err := client.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for DNS failure, got nil")
	}
	t.Logf("Got expected DNS error: %v", err)
}

func TestTLSFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	httpsURL := strings.Replace(srv.URL, "http://", "https://", 1)
	client := New(
		WithBaseURL(httpsURL),
		WithHTTPTimeout(3*time.Second),
	)

	_, err := client.Run(context.Background())
	if err == nil {
		t.Fatal("expected TLS error, got nil")
	}
	t.Logf("Got expected TLS error: %v", err)
}

// =============================================================================
// Edge case: Colo/metadata endpoint failures (non-fatal)
// =============================================================================

func TestColoEndpointFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cdn-cgi/trace":
			http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		case "/__down":
			bytesStr := r.URL.Query().Get("bytes")
			if bytesStr == "0" {
				w.WriteHeader(http.StatusOK)
				return
			}
			size := 1_000_000
			w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
			w.WriteHeader(http.StatusOK)
			chunk := make([]byte, 65536)
			for written := 0; written < size; written += len(chunk) {
				w.Write(chunk)
			}
		case "/__up":
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(2),
		WithDownloadPayload(500_000),
		WithUploadPayload(500_000),
		WithMeasureDuration(2*time.Second),
		WithHTTPTimeout(10*time.Second),
	)

	res, err := client.Run(context.Background())
	if err != nil {
		t.Fatalf("Run should succeed even with colo failure: %v", err)
	}
	if res.DownloadMbps <= 0 {
		t.Errorf("download should be > 0, got %f", res.DownloadMbps)
	}
	t.Logf("Colo failure test: %s", res.JSON())
}

// =============================================================================
// Edge case: Zero-byte endpoints
// =============================================================================

func TestZeroByteDownloadLatency(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithLatencySamples(3),
		WithHTTPTimeout(5*time.Second),
	)

	_, _, err := client.RunLatency(context.Background())
	if err != nil {
		t.Fatalf("zero-byte latency should work: %v", err)
	}
}

// =============================================================================
// Edge case: Single stream
// =============================================================================

func TestSingleStream(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(1),
		WithDownloadPayload(500_000),
		WithUploadPayload(500_000),
		WithMeasureDuration(2*time.Second),
		WithHTTPTimeout(10*time.Second),
	)

	res, err := client.Run(context.Background())
	if err != nil {
		t.Fatalf("single stream test failed: %v", err)
	}
	if res.DownloadMbps <= 0 {
		t.Errorf("download should be > 0 with 1 stream, got %f", res.DownloadMbps)
	}
	if res.UploadMbps <= 0 {
		t.Errorf("upload should be > 0 with 1 stream, got %f", res.UploadMbps)
	}
	t.Logf("Single stream: Download=%.2f, Upload=%.2f", res.DownloadMbps, res.UploadMbps)
}

// =============================================================================
// Edge case: Very short measurement duration
// =============================================================================

func TestVeryShortDuration(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(2),
		WithDownloadPayload(1_000_000),
		WithUploadPayload(1_000_000),
		WithMeasureDuration(500*time.Millisecond),
		WithHTTPTimeout(10*time.Second),
	)

	res, err := client.Run(context.Background())
	if err != nil {
		t.Fatalf("short duration should succeed: %v", err)
	}
	if res.DownloadMbps <= 0 {
		t.Errorf("download should be > 0 even with short duration, got %f", res.DownloadMbps)
	}
	if res.UploadMbps <= 0 {
		t.Errorf("upload should be > 0 even with short duration, got %f", res.UploadMbps)
	}
	t.Logf("Short duration test: %s", res.JSON())
}

// =============================================================================
// Edge case: Minimal payload
// =============================================================================

func TestMinimalPayload(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(2),
		WithDownloadPayload(1_000),
		WithUploadPayload(1_000),
		WithMeasureDuration(2*time.Second),
		WithHTTPTimeout(10*time.Second),
	)

	_, err := client.Run(context.Background())
	if err != nil {
		t.Fatalf("minimal payload should succeed: %v", err)
	}
}

// =============================================================================
// Edge case: Rapid successive tests (connection reuse)
// =============================================================================

func TestRapidSuccessiveTests(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(2),
		WithDownloadPayload(500_000),
		WithUploadPayload(500_000),
		WithMeasureDuration(2*time.Second),
		WithHTTPTimeout(10*time.Second),
	)

	var wg sync.WaitGroup
	errCh := make(chan error, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, err := client.Run(context.Background())
			if err != nil {
				errCh <- fmt.Errorf("test %d: %w", n, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	var errCount int
	for err := range errCh {
		if err != nil {
			t.Error(err)
			errCount++
		}
	}
	if errCount > 0 {
		t.Fatalf("%d/%d concurrent tests failed", errCount, 5)
	}
}

// =============================================================================
// Edge case: Server returns non-200
// =============================================================================

func TestServerErrorResponses(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				<-time.After(500 * time.Millisecond)
				conn.Close()
			}()
		}
	}()
	defer ln.Close()

	client := New(
		WithBaseURL("http://"+ln.Addr().String()),
		WithHTTPTimeout(2*time.Second),
		WithParallelStreams(2),
		WithMeasureDuration(2*time.Second),
	)

	_, err = client.Run(context.Background())
	if err == nil {
		t.Fatal("expected error from unresponsive server")
	}
	t.Logf("Got expected server timeout error: %v", err)
}

// =============================================================================
// Edge case: JSON output consistency
// =============================================================================

func TestJSONOutput(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(2),
		WithDownloadPayload(500_000),
		WithUploadPayload(500_000),
		WithMeasureDuration(2*time.Second),
		WithHTTPTimeout(10*time.Second),
	)

	res, err := client.Run(context.Background())
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	json := res.JSON()
	if !strings.Contains(json, "download_mbps") {
		t.Errorf("JSON missing download_mbps field")
	}
	if !strings.Contains(json, "upload_mbps") {
		t.Errorf("JSON missing upload_mbps field")
	}
	if !strings.Contains(json, "latency_ms") {
		t.Errorf("JSON missing latency_ms field")
	}
	if !strings.Contains(json, "jitter_ms") {
		t.Errorf("JSON missing jitter_ms field")
	}
	if !strings.Contains(json, "download_bytes") {
		t.Errorf("JSON missing download_bytes field (actual bytes)")
	}
	if !strings.Contains(json, "upload_bytes") {
		t.Errorf("JSON missing upload_bytes field (actual bytes)")
	}
	t.Logf("JSON output:\n%s", json)
}

// =============================================================================
// Edge case: Memory / goroutine leak test
// =============================================================================

func TestNoGoroutineLeak(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(4),
		WithDownloadPayload(500_000),
		WithUploadPayload(500_000),
		WithMeasureDuration(2*time.Second),
		WithHTTPTimeout(10*time.Second),
	)

	before := runtime.NumGoroutine()

	for i := 0; i < 3; i++ {
		_, err := client.Run(context.Background())
		if err != nil {
			t.Fatalf("run %d failed: %v", i, err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	after := runtime.NumGoroutine()

	leaked := after - before
	t.Logf("Goroutines before=%d after=%d leaked=%d", before, after, leaked)
	if leaked > 10 {
		t.Errorf("possible goroutine leak: %d goroutines leaked (note: 3-8 idle HTTP conns expected)", leaked)
	}
}

// =============================================================================
// Edge case: Proxy via environment (no actual proxy — test option parsing)
// =============================================================================

func TestProxyOption(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithProxy("http://127.0.0.1:3128"),
		WithHTTPTimeout(2*time.Second),
	)

	_, err := client.Run(context.Background())
	if err == nil {
		t.Log("Proxy test: server responded (unexpected — proxy shouldn't exist)")
	} else {
		t.Logf("Proxy test gave expected connection error: %v", err)
	}
}

// =============================================================================
// Edge case: Insecure TLS option
// =============================================================================

func TestInsecureOptionDoesNotCrash(t *testing.T) {
	client := New(
		WithInsecure(),
		WithHTTPTimeout(1*time.Second),
	)
	if client == nil {
		t.Fatal("New() returned nil with insecure option")
	}
	t.Log("Insecure option: client created successfully")
}

// =============================================================================
// Edge case: Empty context (already cancelled)
// =============================================================================

func TestAlreadyCancelledContext(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithHTTPTimeout(5*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := client.Run(ctx)
	if err == nil {
		t.Log("Got result from cancelled context (latency completed before check?)")
		if res != nil {
			t.Logf("Result: %s", res.JSON())
		}
	} else {
		t.Logf("Cancelled context correctly: %v", err)
	}
}

// =============================================================================
// Edge case: Very large number of parallel streams
// =============================================================================

func TestManyParallelStreams(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(16),
		WithDownloadPayload(100_000),
		WithUploadPayload(100_000),
		WithMeasureDuration(2*time.Second),
		WithHTTPTimeout(10*time.Second),
	)

	res, err := client.Run(context.Background())
	if err != nil {
		t.Fatalf("16-stream test failed: %v", err)
	}
	if res.DownloadMbps <= 0 {
		t.Errorf("download should be > 0, got %f", res.DownloadMbps)
	}
	t.Logf("Many streams test: download=%.2f Mbps", res.DownloadMbps)
}

// =============================================================================
// Edge case: Max parallel is clamped to 64
// =============================================================================

func TestParallelClamp(t *testing.T) {
	// Values above 64 should be clamped.
	c1 := New(WithParallelStreams(999))
	if c1.opts.parallel != maxParallel {
		t.Errorf("expected parallel=%d, got %d", maxParallel, c1.opts.parallel)
	}

	// Values below 1 should be clamped to 1.
	c2 := New(WithParallelStreams(0))
	if c2.opts.parallel != 1 {
		t.Errorf("expected parallel=1, got %d", c2.opts.parallel)
	}

	// Negative values should be clamped to 1.
	c3 := New(WithParallelStreams(-5))
	if c3.opts.parallel != 1 {
		t.Errorf("expected parallel=1, got %d", c3.opts.parallel)
	}

	t.Logf("Parallel clamp: 999→%d, 0→%d, -5→%d", c1.opts.parallel, c2.opts.parallel, c3.opts.parallel)
}

// =============================================================================
// Integration test against real Cloudflare endpoint
// =============================================================================

func TestIntegrationRealCloudflare(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	client := New(
		WithParallelStreams(2),
		WithDownloadPayload(10_000_000),
		WithUploadPayload(10_000_000),
		WithMeasureDuration(5*time.Second),
		WithHTTPTimeout(20*time.Second),
		WithLatencySamples(3),
	)

	res, err := client.Run(context.Background())
	if err != nil {
		t.Fatalf("Real Cloudflare test failed: %v", err)
	}

	t.Logf("=== REAL CLOUDFLARE SPEED TEST ===")
	t.Logf("Server: %s (%s)", res.Server, res.Colo)
	t.Logf("Download: %.2f Mbps (%.2f MB actual)", res.DownloadMbps, float64(res.DownloadBytes)/1_000_000)
	t.Logf("Upload:   %.2f Mbps (%.2f MB actual)", res.UploadMbps, float64(res.UploadBytes)/1_000_000)
	t.Logf("Latency:  %.2f ms (jitter: %.2f ms)", res.LatencyMs, res.JitterMs)
	t.Logf("Loaded latency: down=%.2f ms / up=%.2f ms", res.LoadedLatencyMs, res.UploadLoadedLatencyMs)

	if res.DownloadMbps <= 0 {
		t.Errorf("real download should be > 0")
	}
	if res.Colo == "" {
		t.Errorf("real colo should not be empty")
	}
}

// =============================================================================
// Edge case: Custom base URL
// =============================================================================

func TestCustomBaseURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cdn-cgi/trace":
			w.Write([]byte("colo=CUSTOM\nloc=CUSTOM"))
		case "/__down":
			w.Header().Set("Content-Length", "100000")
			w.WriteHeader(http.StatusOK)
			w.Write(make([]byte, 100000))
		case "/__up":
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(2),
		WithDownloadPayload(100_000),
		WithUploadPayload(100_000),
		WithMeasureDuration(2*time.Second),
		WithHTTPTimeout(10*time.Second),
	)

	res, err := client.Run(context.Background())
	if err != nil {
		t.Fatalf("custom base URL failed: %v", err)
	}
	if res.Colo != "CUSTOM" {
		t.Errorf("expected colo CUSTOM, got %s", res.Colo)
	}
	if res.Server != "CUSTOM" {
		t.Errorf("expected server 'CUSTOM', got '%s'", res.Server)
	}
	if res.DownloadBytes <= 0 {
		t.Errorf("expected download bytes > 0, got %d", res.DownloadBytes)
	}
	if res.UploadBytes <= 0 {
		t.Errorf("expected upload bytes > 0, got %d", res.UploadBytes)
	}
}

// =============================================================================
// Edge case: User-Agent is set on all requests
// =============================================================================

func TestUserAgentIsSet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ua := r.Header.Get("User-Agent")
		if ua != userAgent {
			t.Errorf("expected User-Agent %q, got %q", userAgent, ua)
		}
		switch r.URL.Path {
		case "/cdn-cgi/trace":
			w.Write([]byte("colo=TEST\nloc=TEST"))
		case "/__down":
			w.Header().Set("Content-Length", "1000")
			w.WriteHeader(http.StatusOK)
			w.Write(make([]byte, 1000))
		case "/__up":
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(1),
		WithDownloadPayload(1000),
		WithUploadPayload(1000),
		WithMeasureDuration(1*time.Second),
		WithHTTPTimeout(5*time.Second),
	)

	_, err := client.Run(context.Background())
	if err != nil {
		t.Fatalf("User-Agent test run failed: %v", err)
	}
}

// =============================================================================
// Tests for new features: ParseSize, ProxyParseError, FailedStreams, String output
// =============================================================================

func TestParseSize(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"10MB", 10_000_000},
		{"10MiB", 10_485_760},
		{"1GB", 1_000_000_000},
		{"1GiB", 1_073_741_824},
		{"500KB", 500_000},
		{"512KiB", 524_288},
		{"100B", 100},
		{"10mb", 10_000_000},
		{"10mib", 10_485_760},
		{"1gb", 1_000_000_000},
		{"2gib", 2_147_483_648},
		{"0MB", 0},
	}

	for _, tc := range tests {
		got, err := ParseSize(tc.input)
		if err != nil {
			t.Errorf("ParseSize(%q) unexpected error: %v", tc.input, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseSize(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestParseSizeErrors(t *testing.T) {
	invalid := []string{"", "10", "10XB", "abc"}
	for _, s := range invalid {
		_, err := ParseSize(s)
		if err == nil {
			t.Errorf("ParseSize(%q) expected error, got nil", s)
		}
	}
}

func TestProxyParseError(t *testing.T) {
	c1 := New(WithProxy("http://127.0.0.1:3128"))
	if err := c1.ProxyParseError(); err != nil {
		t.Errorf("expected no error for valid proxy, got: %v", err)
	}

	c2 := New(WithProxy("127.0.0.1:3128"))
	if err := c2.ProxyParseError(); err == nil {
		t.Error("expected error for proxy without scheme, got nil")
	} else {
		t.Logf("Got expected proxy error: %v", err)
	}

	c3 := New(WithProxy("://invalid"))
	if err := c3.ProxyParseError(); err == nil {
		t.Error("expected error for malformed proxy URL, got nil")
	} else {
		t.Logf("Got expected proxy error: %v", err)
	}

	c4 := New()
	if err := c4.ProxyParseError(); err != nil {
		t.Errorf("expected nil for no proxy, got: %v", err)
	}
}

func TestFailedStreamsInResult(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(4),
		WithDownloadPayload(1_000_000),
		WithUploadPayload(1_000_000),
		WithMeasureDuration(2*time.Second),
		WithHTTPTimeout(10*time.Second),
	)

	res, err := client.Run(context.Background())
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if res.FailedStreams < 0 {
		t.Errorf("FailedStreams should be >= 0, got %d", res.FailedStreams)
	}
	t.Logf("FailedStreams: %d (expected 0 for happy path)", res.FailedStreams)
}

func TestResultJSONContainsFailedStreams(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(2),
		WithDownloadPayload(500_000),
		WithUploadPayload(500_000),
		WithMeasureDuration(2*time.Second),
		WithHTTPTimeout(10*time.Second),
	)

	res, err := client.Run(context.Background())
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	json := res.JSON()
	if !strings.Contains(json, "failed_streams") {
		t.Errorf("JSON missing failed_streams field")
	}
	t.Logf("JSON with failed_streams:\n%s", json)
}

func TestStringShowsLoadedLatencyUpload(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := New(
		WithBaseURL(srv.URL),
		WithParallelStreams(2),
		WithDownloadPayload(500_000),
		WithUploadPayload(500_000),
		WithMeasureDuration(2*time.Second),
		WithHTTPTimeout(10*time.Second),
	)

	res, err := client.Run(context.Background())
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	output := res.String()
	if !strings.Contains(output, "/") {
		t.Errorf("String() output should show loaded latency as down/up format")
	}
	t.Logf("String output:\n%s", output)
}
