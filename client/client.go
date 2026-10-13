package client

import (
	"context"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
)

// Range describes a byte range within an object.
type Range struct {
	Offset int64
	Length int64
}

// RangeResult holds the data and timing for a single range read.
type RangeResult struct {
	Range Range
	Data  []byte
	TTFB  time.Duration
	Took  time.Duration
	Err   error
}

// ConnStats tracks connection-level behavior for a client instance.
type ConnStats struct {
	NewConnections   int64 // number of new TCP connections opened
	ReusedConns      int64 // number of requests that reused an existing connection
	HTTP2Connections int64 // number of connections that negotiated HTTP/2
}

// AtomicConnStats is a thread-safe connection statistics collector.
type AtomicConnStats struct {
	newConns  atomic.Int64
	reused    atomic.Int64
	http2     atomic.Int64
}

func (s *AtomicConnStats) RecordNew()    { s.newConns.Add(1) }
func (s *AtomicConnStats) RecordReused() { s.reused.Add(1) }
func (s *AtomicConnStats) RecordHTTP2()  { s.http2.Add(1) }

func (s *AtomicConnStats) Snapshot() ConnStats {
	return ConnStats{
		NewConnections:   s.newConns.Load(),
		ReusedConns:      s.reused.Load(),
		HTTP2Connections: s.http2.Load(),
	}
}

// StorageClient is the common interface implemented by REST and gRPC clients.
type StorageClient interface {
	// Read downloads a full object. Returns data, TTFB, and error.
	Read(ctx context.Context, object string) ([]byte, time.Duration, error)

	// Write uploads an object.
	Write(ctx context.Context, object string, data []byte) error

	// Head fetches object metadata only, no payload.
	Head(ctx context.Context, object string) (*storage.ObjectAttrs, error)

	// List iterates all objects under prefix and returns the count.
	List(ctx context.Context, prefix string) (int, error)

	// ReadRange reads a single byte range from an object. Returns data, TTFB, and error.
	ReadRange(ctx context.Context, object string, offset, length int64) ([]byte, time.Duration, error)

	// ReadRangesPipelined reads multiple ranges concurrently.
	ReadRangesPipelined(ctx context.Context, object string, ranges []Range) ([]RangeResult, error)

	// WriteStreaming writes an object as a stream with the given chunk size.
	WriteStreaming(ctx context.Context, object string, data []byte, chunkSize int) (time.Duration, error)

	// TransportName returns "rest", "rest-pool1", or "grpc".
	TransportName() string

	// ConnStats returns a snapshot of connection-level statistics.
	ConnStats() ConnStats

	Close() error
}
