package client

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptrace"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// RESTClient wraps the official GCS client using the HTTP/2 transport.
type RESTClient struct {
	sc        *storage.Client
	bucket    string
	transport string
	stats     AtomicConnStats
}

// NewRESTClient constructs a GCS client using the default HTTP transport.
// Uses Go's default connection pool which may open multiple TCP connections under concurrency.
func NewRESTClient(ctx context.Context, bucket string) (*RESTClient, error) {
	c := &RESTClient{bucket: bucket, transport: "rest"}
	httpClient, err := newAuthenticatedClient(ctx, 0, &c.stats)
	if err != nil {
		return nil, err
	}
	sc, err := storage.NewClient(ctx, option.WithHTTPClient(httpClient))
	if err != nil {
		return nil, err
	}
	c.sc = sc
	return c, nil
}

// NewRESTClientPool1 constructs a GCS client forced to use a single HTTP/2 connection.
// MaxIdleConnsPerHost=1 and MaxConnsPerHost=1 — all concurrent requests are multiplexed
// as HTTP/2 streams on one TCP connection, equivalent to gRPC's single-channel model.
func NewRESTClientPool1(ctx context.Context, bucket string) (*RESTClient, error) {
	c := &RESTClient{bucket: bucket, transport: "rest-pool1"}
	httpClient, err := newAuthenticatedClient(ctx, 1, &c.stats)
	if err != nil {
		return nil, err
	}
	sc, err := storage.NewClient(ctx, option.WithHTTPClient(httpClient))
	if err != nil {
		return nil, err
	}
	c.sc = sc
	return c, nil
}

// newAuthenticatedClient builds an http.Client with:
//  1. ADC credentials via oauth2.Transport
//  2. httptrace hooks for connection counting
//  3. Optional MaxIdleConnsPerHost/MaxConnsPerHost (0 = use defaults)
func newAuthenticatedClient(ctx context.Context, maxConnsPerHost int, stats *AtomicConnStats) (*http.Client, error) {
	creds, err := google.FindDefaultCredentials(ctx, storage.ScopeFullControl)
	if err != nil {
		return nil, err
	}

	base := &http.Transport{
		ForceAttemptHTTP2: true,
		TLSClientConfig:   &tls.Config{},
	}
	if maxConnsPerHost > 0 {
		base.MaxIdleConnsPerHost = maxConnsPerHost
		base.MaxConnsPerHost = maxConnsPerHost
	}

	traced := &tracedRoundTripper{base: base, stats: stats}

	// oauth2.Transport adds Authorization header; traced transport handles connection tracking
	authTransport := &oauth2.Transport{
		Source: creds.TokenSource,
		Base:   traced,
	}

	return &http.Client{Transport: authTransport}, nil
}

// tracedRoundTripper wraps an http.RoundTripper with httptrace hooks to count connections.
type tracedRoundTripper struct {
	base  http.RoundTripper
	stats *AtomicConnStats
}

func (t *tracedRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Reused {
				t.stats.RecordReused()
			} else {
				t.stats.RecordNew()
			}
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			if err == nil && state.NegotiatedProtocol == "h2" {
				t.stats.RecordHTTP2()
			}
		},
	}
	return t.base.RoundTrip(req.WithContext(httptrace.WithClientTrace(req.Context(), trace)))
}

func (c *RESTClient) TransportName() string { return c.transport }

func (c *RESTClient) ConnStats() ConnStats { return c.stats.Snapshot() }

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
