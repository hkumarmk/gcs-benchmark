package client

import (
	"context"
	"io"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// RESTClient wraps the official GCS client using the default HTTP/2 transport.
type RESTClient struct {
	sc     *storage.Client
	bucket string
}

// NewRESTClient constructs a GCS client using the HTTP transport.
// google-cloud-go/storage uses HTTP/2 by default via ALPN negotiation.
// Credentials are resolved via Application Default Credentials.
func NewRESTClient(ctx context.Context, bucket string) (*RESTClient, error) {
	sc, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return &RESTClient{sc: sc, bucket: bucket}, nil
}

func (c *RESTClient) TransportName() string { return "rest" }

func (c *RESTClient) Read(ctx context.Context, object string) ([]byte, time.Duration, error) {
	r, err := c.sc.Bucket(c.bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer r.Close()

	var ttfb time.Duration
	start := time.Now()
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

func (c *RESTClient) Write(ctx context.Context, object string, data []byte) error {
	w := c.sc.Bucket(c.bucket).Object(object).NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

func (c *RESTClient) Head(ctx context.Context, object string) (*storage.ObjectAttrs, error) {
	return c.sc.Bucket(c.bucket).Object(object).Attrs(ctx)
}

func (c *RESTClient) List(ctx context.Context, prefix string) (int, error) {
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

func (c *RESTClient) ReadRange(ctx context.Context, object string, offset, length int64) ([]byte, time.Duration, error) {
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

// ReadRangesPipelined issues all range reads concurrently — N goroutines, all in-flight simultaneously
// over the shared HTTP/2 connection pool. This is the best achievable REST pattern for pipelined reads.
func (c *RESTClient) ReadRangesPipelined(ctx context.Context, object string, ranges []Range) ([]RangeResult, error) {
	results := make([]RangeResult, len(ranges))
	var wg sync.WaitGroup

	for i, rng := range ranges {
		wg.Add(1)
		go func(idx int, r Range) {
			defer wg.Done()
			start := time.Now()
			data, ttfb, err := c.ReadRange(ctx, object, r.Offset, r.Length)
			results[idx] = RangeResult{
				Range: r,
				Data:  data,
				TTFB:  ttfb,
				Took:  time.Since(start),
				Err:   err,
			}
		}(i, rng)
	}

	wg.Wait()
	return results, nil
}

// WriteStreaming uses GCS resumable upload — POST to initiate session, stream body in chunks.
// Returns time to first server acknowledgement (TTFB equivalent for writes).
func (c *RESTClient) WriteStreaming(ctx context.Context, object string, data []byte, chunkSize int) (time.Duration, error) {
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

func (c *RESTClient) Close() error {
	return c.sc.Close()
}
