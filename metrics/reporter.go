package metrics

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// WriteJSON writes a single Result to a JSON file in outputDir.
func WriteJSON(r Result, iters []Iteration, outputDir string) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}

	sizeLabel := strings.ReplaceAll(r.ObjectSizeLabel, "/", "-")
	filename := fmt.Sprintf("%s_%s_%s_c%d.json",
		r.Scenario, r.Transport, sizeLabel, r.Concurrency)

	type output struct {
		Result
		RawIterations []Iteration `json:"raw_iterations"`
	}

	f, err := os.Create(filepath.Join(outputDir, filename))
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(output{Result: r, RawIterations: iters})
}

// WriteCSVSummary appends all results to a single summary CSV file.
func WriteCSVSummary(results []Result, outputDir string) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(outputDir, "summary.csv"))
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"scenario", "transport", "size_label", "size_bytes", "concurrency",
		"iterations", "errors",
		"p50_latency_ms", "p95_latency_ms", "p99_latency_ms",
		"min_latency_ms", "max_latency_ms",
		"p50_ttfb_ms", "p95_ttfb_ms",
		"per_op_bw_mbps", "agg_throughput_mbps", "ops_per_sec", "avg_cpu_pct",
	}
	if err := w.Write(header); err != nil {
		return err
	}

	for _, r := range results {
		row := []string{
			r.Scenario, r.Transport, r.ObjectSizeLabel,
			strconv.FormatInt(r.ObjectSizeBytes, 10),
			strconv.Itoa(r.Concurrency),
			strconv.Itoa(r.Iterations),
			strconv.Itoa(r.ErrorCount),
			ff(r.P50LatencyMs), ff(r.P95LatencyMs), ff(r.P99LatencyMs),
			ff(r.MinLatencyMs), ff(r.MaxLatencyMs),
			ff(r.P50TTFBMs), ff(r.P95TTFBMs),
			ff(r.PerOpBandwidthMBs), ff(r.ThroughputMBps), ff(r.OpsPerSec), ff(r.AvgCPUPercent),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

// PrintTable prints a formatted comparison table to stdout with delta rows.
// MB/s column is per-operation bandwidth (objectSize/p50Latency) — independent of concurrency.
// Use summary.csv for aggregate throughput (agg_throughput_mbps) and ops/sec.
func PrintTable(results []Result) {
	fmt.Printf("\n%-20s %-8s %-12s %-6s %8s %8s %8s %8s %8s %6s\n",
		"Scenario", "Size", "Concurrency", "Client",
		"P50(ms)", "P95(ms)", "P99(ms)", "MB/s*", "OPS/s", "CPU%")
	fmt.Printf("  * MB/s = per-op bandwidth (objectSize/p50Latency) — not affected by concurrency level\n")
	fmt.Println(repeat("─", 102))

	// Group results in pairs (REST then gRPC) for delta rows
	type key struct{ scenario, size string; concurrency int }
	byKey := make(map[key][2]*Result) // index 0=rest, 1=grpc

	for i := range results {
		r := &results[i]
		k := key{r.Scenario, r.ObjectSizeLabel, r.Concurrency}
		entry := byKey[k]
		if r.Transport == "rest" {
			entry[0] = r
		} else {
			entry[1] = r
		}
		byKey[k] = entry
	}

	for _, r := range results {
		fmt.Printf("%-20s %-8s %-12d %-6s %8.1f %8.1f %8.1f %8.2f %8.1f %6.1f\n",
			r.Scenario, r.ObjectSizeLabel, r.Concurrency, r.Transport,
			r.P50LatencyMs, r.P95LatencyMs, r.P99LatencyMs,
			r.PerOpBandwidthMBs, r.OpsPerSec, r.AvgCPUPercent)

		k := key{r.Scenario, r.ObjectSizeLabel, r.Concurrency}
		pair := byKey[k]
		if r.Transport == "grpc" && pair[0] != nil && pair[1] != nil {
			rest, grpc := pair[0], pair[1]
			fmt.Printf("%-20s %-8s %-12d %-6s %8s %8s %8s %8s %8s\n",
				"", "", r.Concurrency, "DELTA",
				pct(rest.P50LatencyMs, grpc.P50LatencyMs),
				pct(rest.P95LatencyMs, grpc.P95LatencyMs),
				pct(rest.P99LatencyMs, grpc.P99LatencyMs),
				pct(rest.PerOpBandwidthMBs, grpc.PerOpBandwidthMBs),
				pct(rest.OpsPerSec, grpc.OpsPerSec))
			fmt.Println()
		}
	}
}

func ff(f float64) string { return strconv.FormatFloat(f, 'f', 2, 64) }

func pct(baseline, measured float64) string {
	if baseline == 0 {
		return "n/a"
	}
	d := (measured - baseline) / baseline * 100
	if d < 0 {
		return fmt.Sprintf("%.1f%%", d) // negative = faster (lower latency)
	}
	return fmt.Sprintf("+%.1f%%", d)
}

func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}
