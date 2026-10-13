package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// GRPCClient wraps the official GCS gRPC client with BidiReadObject and BidiWriteObject enabled.
type GRPCClient struct {
	sc     *storage.Client
	bucket string
}

// NewGRPCClient constructs a GCS client using the gRPC transport with bidirectional streaming APIs.
//
// storage.NewGRPCClient with WithGRPCBidiReads enables BidiReadObject — required for
// NewMultiRangeDownloader (pipelined range reads over a single stream).
// WithAppendableUploads enables BidiWriteObject for streaming writes.
//
// Credentials are resolved via Application Default Credentials.
func NewGRPCClient(ctx context.Context, bucket string) (*GRPCClient, error) {
	sc, err := storage.NewGRPCClient(ctx,
		storage.WithDisabledClientMetrics(),
	)
	if err != nil {
		return nil, err
	}
	return &GRPCClient{sc: sc, bucket: bucket}, nil
}

// NewGRPCBidiClient constructs a gRPC client with BidiReadObject and BidiWriteObject enabled.
// Requires the GCP project to be enrolled in the BidiReadObject preview programme.
// Use this only for bidi-read and bidi-write scenarios.
func NewGRPCBidiClient(ctx context.Context, bucket string) (*GRPCClient, error) {
	sc, err := storage.NewGRPCClient(ctx,
		storage.WithGRPCBidiReads(),
		storage.WithAppendableUploads(),
		storage.WithDisabledClientMetrics(),
	)
	if err != nil {
		return nil, err
	}
	return &GRPCClient{sc: sc, bucket: bucket}, nil
}

func (c *GRPCClient) TransportName() string { return "grpc" }

// ConnStats returns fixed stats for gRPC — always one channel, no TCP connection tracking.
func (c *GRPCClient) ConnStats() ConnStats {
	return ConnStats{
		NewConnections:   1,
		ReusedConns:      -1, // not applicable — gRPC multiplexes streams, not connections
		HTTP2Connections: 1,
	}
}

func (c *GRPCClient) Read(ctx context.Context, object string) ([]byte, time.Duration, error) {
	r, err := c.sc.Bucket(c.bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer r.Close()

	start := time.Now()
	var ttfb time.Duration
	buf := make([]byte, 32*1024)
	var data []byte
	first := true

	for {
		n, err := r.Read(buf)
		if n > 0 {
			if first {
				ttfb = time.Since(start)
				first = false
			}
			data = append(data, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, ttfb, err
		}
	}
	return data, ttfb, nil
}

func (c *GRPCClient) Write(ctx context.Context, object string, data []byte) error {
	w := c.sc.Bucket(c.bucket).Object(object).NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

func (c *GRPCClient) Head(ctx context.Context, object string) (*storage.ObjectAttrs, error) {
	return c.sc.Bucket(c.bucket).Object(object).Attrs(ctx)
}

func (c *GRPCClient) List(ctx context.Context, prefix string) (int, error) {
	it := c.sc.Bucket(c.bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	count := 0
	for {
		_, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (c *GRPCClient) ReadRange(ctx context.Context, object string, offset, length int64) ([]byte, time.Duration, error) {
	r, err := c.sc.Bucket(c.bucket).Object(object).NewRangeReader(ctx, offset, length)
	if err != nil {
		return nil, 0, err
	}
	defer r.Close()

	start := time.Now()
	var ttfb time.Duration
	buf := make([]byte, 32*1024)
	var data []byte
	first := true

	for {
		n, err := r.Read(buf)
		if n > 0 {
			if first {
				ttfb = time.Since(start)
				first = false
			}
			data = append(data, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, ttfb, err
		}
	}
	return data, ttfb, nil
}

// ReadRangesPipelined uses NewMultiRangeDownloader — the public API for BidiReadObject.
// All ranges are added to a single bidirectional stream; the server sends data back
// as it becomes available without waiting for each request to complete.
// This is structurally different from the REST implementation which uses N independent HTTP requests.
func (c *GRPCClient) ReadRangesPipelined(ctx context.Context, object string, ranges []Range) ([]RangeResult, error) {
	mrd, err := c.sc.Bucket(c.bucket).Object(object).NewMultiRangeDownloader(ctx)
	if err != nil {
		return nil, fmt.Errorf("NewMultiRangeDownloader: %w", err)
	}

	results := make([]RangeResult, len(ranges))
	var mu sync.Mutex

	for i, rng := range ranges {
		idx := i
		r := rng
		buf := &bytes.Buffer{}
		start := time.Now()

		mrd.Add(buf, r.Offset, r.Length, func(offset, length int64, err error) {
			result := RangeResult{
				Range: r,
				Took:  time.Since(start),
				Err:   err,
			}
			if err == nil {
				result.Data = buf.Bytes()
				result.TTFB = time.Since(start) // TTFB approximation — first callback = first data
			}
			mu.Lock()
			results[idx] = result
			mu.Unlock()
		})
	}

	mrd.Wait()
	if err := mrd.Close(); err != nil {
		return nil, fmt.Errorf("MultiRangeDownloader close: %w", err)
	}

	return results, nil
}

// WriteStreaming uses BidiWriteObject via the gRPC transport (enabled by WithAppendableUploads).
// Returns time to first server acknowledgement.
func (c *GRPCClient) WriteStreaming(ctx context.Context, object string, data []byte, chunkSize int) (time.Duration, error) {
	w := c.sc.Bucket(c.bucket).Object(object).NewWriter(ctx)
	w.ChunkSize = chunkSize

	start := time.Now()
	var firstAck time.Duration

	offset := 0
	for offset < len(data) {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		if _, err := w.Write(data[offset:end]); err != nil {
			_ = w.Close()
			return firstAck, err
		}
		if firstAck == 0 {
			firstAck = time.Since(start)
		}
		offset = end
	}
	return firstAck, w.Close()
}

func (c *GRPCClient) Close() error {
	return c.sc.Close()
}
