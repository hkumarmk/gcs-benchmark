package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/storage-benchmark/gcs-benchmark/client"
	"github.com/storage-benchmark/gcs-benchmark/setup"
)

const (
	rangeObjectSize = 100 * 1024 * 1024 // 100MB
	rangeSize       = 1 * 1024 * 1024   // 1MB per range
)

// BidiRead returns a ScenarioFn for pipelined range reads from the 100MB fixture object.
// numRanges is the number of concurrent outstanding range requests — corresponds to concurrency level.
//
// REST:  N goroutines each issuing one HTTP Range request concurrently over the shared HTTP/2 pool.
// gRPC:  N goroutines issuing concurrent range reads; the gRPC transport multiplexes over one stream.
//
// Requires BidiReadObject private preview access — use --scenarios bidi-read explicitly.
func BidiRead(numRanges int) ScenarioFn {
	ranges := buildRanges(numRanges)
	key := setup.RangePrefix + "100m"

	return func(ctx context.Context, c client.StorageClient, workerID int) (time.Duration, int64, error) {
		results, err := c.ReadRangesPipelined(ctx, key, ranges)
		if err != nil {
			return 0, 0, fmt.Errorf("pipelined range read: %w", err)
		}

		var minTTFB time.Duration
		var totalBytes int64
		for _, r := range results {
			if r.Err != nil {
				return 0, 0, fmt.Errorf("range read error: %w", r.Err)
			}
			totalBytes += int64(len(r.Data))
			if minTTFB == 0 || r.TTFB < minTTFB {
				minTTFB = r.TTFB
			}
		}
		return minTTFB, totalBytes, nil
	}
}

// buildRanges constructs N non-overlapping 1MB ranges evenly distributed across the 100MB object.
func buildRanges(n int) []client.Range {
	ranges := make([]client.Range, n)
	step := int64(rangeObjectSize / n)
	for i := 0; i < n; i++ {
		ranges[i] = client.Range{
			Offset: int64(i) * step,
			Length: rangeSize,
		}
	}
	return ranges
}
