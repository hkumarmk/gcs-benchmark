package metrics

import (
	"fmt"
	"time"

	"github.com/montanaflynn/stats"
)

// Result holds aggregated statistics for one (scenario, transport, size, concurrency) combination.
type Result struct {
	Scenario        string
	Transport       string
	ObjectSizeBytes int64
	ObjectSizeLabel string
	Concurrency     int
	Iterations      int
	ErrorCount      int

	P50LatencyMs float64
	P95LatencyMs float64
	P99LatencyMs float64
	MinLatencyMs float64
	MaxLatencyMs float64

	P50TTFBMs float64
	P95TTFBMs float64

	ThroughputMBps float64
	OpsPerSec      float64

	AvgCPUPercent float64 // populated externally by scenario runner
}

// Aggregate computes statistics from a slice of iterations.
// totalWallTime is the wall-clock duration from first goroutine start to last finish —
// used for throughput calculation across concurrent workers.
func Aggregate(iters []Iteration, totalWallTime time.Duration) Result {
	if len(iters) == 0 {
		return Result{}
	}

	first := iters[0]
	r := Result{
		Scenario:        first.Scenario,
		Transport:       first.Transport,
		ObjectSizeBytes: first.ObjectSize,
		ObjectSizeLabel: sizeLabel(first.ObjectSize),
		Concurrency:     first.Concurrency,
		Iterations:      len(iters),
	}

	var latencies, ttfbs stats.Float64Data
	var totalBytes int64

	for _, it := range iters {
		if it.Error != "" {
			r.ErrorCount++
			continue
		}
		latencies = append(latencies, float64(it.TotalLatency.Milliseconds()))
		if it.TTFB > 0 {
			ttfbs = append(ttfbs, float64(it.TTFB.Milliseconds()))
		}
		totalBytes += it.BytesXferred
	}

	if len(latencies) > 0 {
		r.P50LatencyMs, _ = latencies.Percentile(50)
		r.P95LatencyMs, _ = latencies.Percentile(95)
		r.P99LatencyMs, _ = latencies.Percentile(99)
		r.MinLatencyMs, _ = latencies.Min()
		r.MaxLatencyMs, _ = latencies.Max()
	}

	if len(ttfbs) > 0 {
		r.P50TTFBMs, _ = ttfbs.Percentile(50)
		r.P95TTFBMs, _ = ttfbs.Percentile(95)
	}

	if totalWallTime > 0 && totalBytes > 0 {
		wallSecs := totalWallTime.Seconds()
		r.ThroughputMBps = float64(totalBytes) / wallSecs / (1024 * 1024)
	}

	if totalWallTime > 0 && len(latencies) > 0 {
		r.OpsPerSec = float64(len(latencies)) / totalWallTime.Seconds()
	}

	return r
}

// SizeLabel returns a human-readable label for a byte count.
func SizeLabel(bytes int64) string { return sizeLabel(bytes) }

func sizeLabel(bytes int64) string {
	switch {
	case bytes == 0:
		return "n/a"
	case bytes < 1024:
		return fmt.Sprintf("%dB", bytes)
	case bytes < 1024*1024:
		return fmt.Sprintf("%dKB", bytes/1024)
	default:
		return fmt.Sprintf("%dMB", bytes/(1024*1024))
	}
}
