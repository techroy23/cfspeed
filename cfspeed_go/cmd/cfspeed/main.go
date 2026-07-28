package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/techroy23/cfspeed/cfspeed_go"
)

func main() {
	var (
		parallel      = flag.Int("parallel", 4, "number of parallel streams (max 64)")
		duration      = flag.Duration("duration", 10*time.Second, "per-test measurement duration")
		payloadSize   = flag.String("size", "10MB", "payload size per stream (e.g. 10MB, 50MB, 1GB)")
		latencyProbes = flag.Int("latency-samples", 5, "number of latency probes")
		jsonOutput    = flag.Bool("json", false, "output as JSON")
		insecure      = flag.Bool("insecure", false, "skip TLS verification")
		proxy         = flag.String("proxy", "", "proxy URL (http/socks5)")
		timeout       = flag.Duration("timeout", 30*time.Second, "HTTP client timeout")
	)
	flag.Parse()

	bytes, err := cfspeed.ParseSize(*payloadSize)
	if err != nil {
		log.Fatalf("Invalid size: %v", err)
	}

	opts := []cfspeed.ClientOption{
		cfspeed.WithParallelStreams(*parallel),
		cfspeed.WithMeasureDuration(*duration),
		cfspeed.WithDownloadPayload(bytes),
		cfspeed.WithUploadPayload(bytes),
		cfspeed.WithLatencySamples(*latencyProbes),
		cfspeed.WithHTTPTimeout(*timeout),
	}

	if *insecure {
		opts = append(opts, cfspeed.WithInsecure())
	}
	if *proxy != "" {
		opts = append(opts, cfspeed.WithProxy(*proxy))
	}

	client := cfspeed.New(opts...)

	if proxyParseErr := client.ProxyParseError(); proxyParseErr != nil {
		log.Printf("Warning: %v (test will continue without proxy)", proxyParseErr)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	result, err := client.Run(ctx)
	if err != nil {
		log.Fatalf("Speed test failed: %v", err)
	}

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(result)
	} else {
		fmt.Println(result.String())
	}
}
