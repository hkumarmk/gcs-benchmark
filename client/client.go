package client

import (
	"context"
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

// StorageClient is the common interface implemented by both REST and gRPC clients.
// Scenario code calls this interface and has no knowledge of the underlying transport.
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
	// REST: N concurrent goroutines each issuing one HTTP Range request.
	// gRPC: single BidiReadObject stream with N outstanding requests.
	ReadRangesPipelined(ctx context.Context, object string, ranges []Range) ([]RangeResult, error)

	// WriteStreaming writes an object as a stream with the given chunk size.
	// REST: GCS resumable upload.
	// gRPC: BidiWriteObject.
	WriteStreaming(ctx context.Context, object string, data []byte, chunkSize int) (time.Duration, error)

	// TransportName returns "rest" or "grpc" — used in metric labelling.
	TransportName() string

	Close() error
}
