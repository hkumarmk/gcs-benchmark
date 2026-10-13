// grpc-check verifies connectivity and protocol negotiation before running the full benchmark.
//
// It checks:
//  1. REST client HTTP/2 negotiation via TLS ALPN
//  2. gRPC client basic read and head operations
//
// Usage:
//
//	go run ./cmd/grpc-check --bucket BUCKET --project PROJECT
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httptrace"
	"time"

	"cloud.google.com/go/storage"
)

func main() {
	bucket := flag.String("bucket", "", "GCS bucket name (must already exist)")
	project := flag.String("project", "", "GCP project ID")
	flag.Parse()

	if *bucket == "" || *project == "" {
		log.Fatal("--bucket and --project are required")
	}

	checkHTTP2()

	ctx := context.Background()

	// Write a small object via REST first
	fmt.Println("\nCreating REST client...")
	restClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("REST client: %v", err)
	}
	defer restClient.Close()

	key := fmt.Sprintf("grpc-check-%d", time.Now().UnixNano())
	payload := []byte("hello grpc benchmark")

	fmt.Printf("Uploading test object %q via REST...\n", key)
	w := restClient.Bucket(*bucket).Object(key).NewWriter(ctx)
	if _, err := w.Write(payload); err != nil {
		log.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		log.Fatalf("write close: %v", err)
	}
	fmt.Println("  OK")

	defer func() {
		fmt.Printf("Deleting test object %q...\n", key)
		if err := restClient.Bucket(*bucket).Object(key).Delete(ctx); err != nil {
			fmt.Printf("  warning: delete failed: %v\n", err)
		} else {
			fmt.Println("  OK")
		}
	}()

	// Read it back via gRPC
	fmt.Println("\nCreating gRPC client (standard, no BidiReadObject)...")
	grpcClient, err := storage.NewGRPCClient(ctx,
		storage.WithDisabledClientMetrics(),
	)
	if err != nil {
		log.Fatalf("gRPC client: %v", err)
	}
	defer grpcClient.Close()

	fmt.Printf("Reading object %q via gRPC...\n", key)
	start := time.Now()
	r, err := grpcClient.Bucket(*bucket).Object(key).NewReader(ctx)
	if err != nil {
		log.Fatalf("gRPC NewReader: %v", err)
	}
	buf := make([]byte, 1024)
	n, err := r.Read(buf)
	r.Close()
	if err != nil && err.Error() != "EOF" {
		log.Fatalf("gRPC Read: %v", err)
	}
	fmt.Printf("  OK — read %d bytes in %s: %q\n", n, time.Since(start), buf[:n])

	fmt.Printf("Fetching metadata for %q via gRPC...\n", key)
	start = time.Now()
	attrs, err := grpcClient.Bucket(*bucket).Object(key).Attrs(ctx)
	if err != nil {
		log.Fatalf("gRPC Attrs: %v", err)
	}
	fmt.Printf("  OK — size=%d updated=%s took=%s\n", attrs.Size, attrs.Updated.Format(time.RFC3339), time.Since(start))

	fmt.Println("\nAll checks passed.")
}

// checkHTTP2 opens a raw TLS connection to storage.googleapis.com and checks
// which protocol was negotiated via ALPN. This confirms whether the REST client
// will use HTTP/2 or fall back to HTTP/1.1.
func checkHTTP2() {
	fmt.Println("=== Checking HTTP/2 negotiation for REST transport ===")

	// Use httptrace to capture the negotiated protocol
	negotiated := ""
	trace := &httptrace.ClientTrace{
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			if err == nil {
				negotiated = state.NegotiatedProtocol
			}
		},
	}

	req, _ := http.NewRequest("HEAD", "https://storage.googleapis.com/", nil)
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{},
			ForceAttemptHTTP2: true,
		},
		Timeout: 10 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("  WARNING: HTTP probe failed: %v\n", err)
		fmt.Println("  Cannot confirm HTTP/2 — benchmark REST results may be HTTP/1.1")
		return
	}
	defer resp.Body.Close()

	if negotiated == "h2" {
		fmt.Printf("  OK — ALPN negotiated: %q (HTTP/2 confirmed)\n", negotiated)
	} else if negotiated == "" {
		fmt.Printf("  WARNING — no ALPN protocol negotiated (possible proxy or HTTP/1.1 fallback)\n")
	} else {
		fmt.Printf("  WARNING — negotiated %q, not h2 — REST client will use HTTP/1.1, benchmark comparison is unfair\n", negotiated)
	}
}
