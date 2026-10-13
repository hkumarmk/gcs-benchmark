package scenarios

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/storage-benchmark/gcs-benchmark/client"
	"github.com/storage-benchmark/gcs-benchmark/metrics"
)

// ScenarioFn is a single benchmark operation. It returns TTFB, bytes transferred, and error.
type ScenarioFn func(ctx context.Context, c client.StorageClient) (ttfb time.Duration, bytes int64, err error)

// RunScenario executes warmup iterations (discarded) then measurement iterations
// using a goroutine pool of size concurrency. Returns collected iterations and total wall time.
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

	// Warmup — discard results
	fmt.Printf("  warmup (%d iterations)...\n", warmup)
	runBatch(ctx, c, concurrency, warmup, fn, nil)

	// Measurement
	fmt.Printf("  measuring (%d iterations, concurrency=%d)...\n", iterations, concurrency)
	results := make([]metrics.Iteration, 0, iterations)
	mu := &sync.Mutex{}

	wallStart := time.Now()
	runBatch(ctx, c, concurrency, iterations, fn, func(num int, ttfb time.Duration, bytes int64, lat time.Duration, err error) {
		it := metrics.Iteration{
			Scenario:     scenarioName,
			Transport:    c.TransportName(),
			ObjectSize:   objectSize,
			Concurrency:  concurrency,
			IterationNum: num,
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
	})
	wallTime := time.Since(wallStart)

	errorCount := 0
	for _, r := range results {
		if r.Error != "" {
			errorCount++
		}
	}
	if errorCount > 0 {
		errPct := float64(errorCount) / float64(len(results)) * 100
		fmt.Printf("  warning: %d errors (%.1f%%)\n", errorCount, errPct)
		if errPct > 10 {
			fmt.Printf("  WARNING: error rate exceeds 10%% — results may be unreliable\n")
		}
	}

	return results, wallTime
}

type recordFn func(num int, ttfb time.Duration, bytes int64, lat time.Duration, err error)

func runBatch(ctx context.Context, c client.StorageClient, concurrency, total int, fn ScenarioFn, record recordFn) {
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for i := 0; i < total; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(num int) {
			defer wg.Done()
			defer func() { <-sem }()

			start := time.Now()
			ttfb, bytes, err := fn(ctx, c)
			lat := time.Since(start)

			if record != nil {
				record(num, ttfb, bytes, lat, err)
			}
		}(i)
	}
	wg.Wait()
}
