package scenarios

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/storage-benchmark/gcs-benchmark/client"
	"github.com/storage-benchmark/gcs-benchmark/metrics"
)

// ScenarioFn is a single benchmark operation.
// workerID identifies the goroutine (0-based) so scenarios can select a per-worker object key.
// Returns TTFB, bytes transferred, and error.
type ScenarioFn func(ctx context.Context, c client.StorageClient, workerID int) (ttfb time.Duration, bytes int64, err error)

// RunScenario launches exactly concurrency goroutines, each running iterations times independently.
// Total measurements = concurrency * iterations.
// Warmup: each worker runs warmup iterations first and discards results.
func RunScenario(
	ctx context.Context,
	scenarioName string,
	c client.StorageClient,
	objectSize int64,
	concurrency int,
	warmup int,
	iterations int,
	fn ScenarioFn,
) ([]metrics.Iteration, time.Duration) {

	total := concurrency * iterations
	results := make([]metrics.Iteration, 0, total)
	mu := &sync.Mutex{}
	var wg sync.WaitGroup

	fmt.Printf("  warmup (%d per worker) + measuring (%d per worker, concurrency=%d, total=%d)...\n",
		warmup, iterations, concurrency, total)

	wallStart := time.Now()

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Warmup — discard results
			for i := 0; i < warmup; i++ {
				fn(ctx, c, workerID) //nolint:errcheck
			}

			// Measurement
			for i := 0; i < iterations; i++ {
				start := time.Now()
				ttfb, bytes, err := fn(ctx, c, workerID)
				lat := time.Since(start)

				it := metrics.Iteration{
					Scenario:     scenarioName,
					Transport:    c.TransportName(),
					ObjectSize:   objectSize,
					Concurrency:  concurrency,
					IterationNum: workerID*iterations + i,
					TTFB:         ttfb,
					TotalLatency: lat,
					BytesXferred: bytes,
				}
				if err != nil {
					it.Error = err.Error()
				}
				mu.Lock()
				results = append(results, it)
				mu.Unlock()
			}
		}(w)
	}

	wg.Wait()
	wallTime := time.Since(wallStart)

	errorCount := 0
	firstError := ""
	for _, r := range results {
		if r.Error != "" {
			errorCount++
			if firstError == "" {
				firstError = r.Error
			}
		}
	}
	if errorCount > 0 {
		errPct := float64(errorCount) / float64(len(results)) * 100
		fmt.Printf("  warning: %d errors (%.1f%%) — first error: %s\n", errorCount, errPct, firstError)
		if errPct > 10 {
			fmt.Printf("  WARNING: error rate exceeds 10%% — results may be unreliable\n")
		}
	}

	return results, wallTime
}
