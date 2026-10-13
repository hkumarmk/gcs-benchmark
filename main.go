package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"time"

	"cloud.google.com/go/storage"
	"github.com/storage-benchmark/gcs-benchmark/client"
	"github.com/storage-benchmark/gcs-benchmark/config"
	"github.com/storage-benchmark/gcs-benchmark/metrics"
	"github.com/storage-benchmark/gcs-benchmark/scenarios"
	"github.com/storage-benchmark/gcs-benchmark/setup"
)

func main() {
	cfg, err := config.Parse()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	printRunInfo(cfg)

	ctx := context.Background()

	// Shared storage client for setup/teardown only
	setupClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("create setup client: %v", err)
	}
	defer setupClient.Close()

	if cfg.Setup {
		if err := setup.Setup(ctx, setupClient, cfg.Project, cfg.Bucket, cfg.Region); err != nil {
			log.Fatalf("setup: %v", err)
		}
	}

	// Always attempt teardown, even on error
	if cfg.Teardown {
		defer func() {
			fmt.Println("\nRunning teardown...")
			if err := setup.Teardown(ctx, setupClient, cfg.Bucket); err != nil {
				fmt.Fprintf(os.Stderr, "teardown error: %v\n", err)
			}
		}()
	}

	var allResults []metrics.Result

	for _, scenarioName := range cfg.Scenarios {
		for _, transport := range cfg.Clients {
			c, err := buildClient(ctx, transport, cfg.Bucket)
			if err != nil {
				log.Fatalf("build %s client: %v", transport, err)
			}

			// bidi-write iterates chunk sizes as an additional dimension
			if scenarioName == "bidi-write" {
				for _, chunkSize := range scenarios.ChunkSizes {
					for _, conc := range concurrencyFor(scenarioName, cfg.Concurrency) {
						label := fmt.Sprintf("bidi-write-chunk%dk", chunkSize/1024)
						fmt.Printf("\n[%s] transport=%s chunkSize=%dk concurrency=%d\n",
							scenarioName, transport, chunkSize/1024, conc)
						fn := scenarios.BidiWrite(chunkSize)
						iters, wallTime := scenarios.RunScenario(
							ctx, label, c, 100*1024*1024, conc,
							cfg.Warmup, cfg.Iterations, fn,
						)
						result := metrics.Aggregate(iters, wallTime)
						allResults = append(allResults, result)
						if cfg.OutputFormat == "json" || cfg.OutputFormat == "both" {
							if err := metrics.WriteJSON(result, iters, cfg.OutputDir); err != nil {
								fmt.Fprintf(os.Stderr, "write json: %v\n", err)
							}
						}
					}
				}
				continue
			}

			for _, size := range scenariosNeedSize(scenarioName, cfg.Sizes) {
				for _, conc := range concurrencyFor(scenarioName, cfg.Concurrency) {
					fmt.Printf("\n[%s] transport=%s size=%s concurrency=%d\n",
						scenarioName, transport, metrics.SizeLabel(size), conc)

					fn, objectSize := buildScenarioFn(scenarioName, size, conc)
					if fn == nil {
						continue
					}

					iters, wallTime := scenarios.RunScenario(
						ctx, scenarioName, c, objectSize, conc,
						cfg.Warmup, cfg.Iterations, fn,
					)

					result := metrics.Aggregate(iters, wallTime)
					allResults = append(allResults, result)

					if cfg.OutputFormat == "json" || cfg.OutputFormat == "both" {
						if err := metrics.WriteJSON(result, iters, cfg.OutputDir); err != nil {
							fmt.Fprintf(os.Stderr, "write json: %v\n", err)
						}
					}
				}
			}

			c.Close()
		}

		fmt.Printf("\nIdling %s between scenarios...\n", cfg.IdleBetween)
		time.Sleep(cfg.IdleBetween)
	}

	if cfg.OutputFormat == "csv" || cfg.OutputFormat == "both" {
		if err := metrics.WriteCSVSummary(allResults, cfg.OutputDir); err != nil {
			fmt.Fprintf(os.Stderr, "write csv: %v\n", err)
		}
	}

	metrics.PrintTable(allResults)
	fmt.Printf("\nResults written to %s\n", cfg.OutputDir)
}

func buildClient(ctx context.Context, transport, bucket string) (client.StorageClient, error) {
	switch transport {
	case "rest":
		return client.NewRESTClient(ctx, bucket)
	case "grpc":
		return client.NewGRPCClient(ctx, bucket)
	default:
		return nil, fmt.Errorf("unknown transport %q", transport)
	}
}

func buildScenarioFn(name string, size int64, concurrency int) (scenarios.ScenarioFn, int64) {
	switch name {
	case "read":
		return scenarios.ReadObject(size), size
	case "write":
		return scenarios.WriteObject(size), size
	case "head":
		return scenarios.HeadObject(), 0
	case "list":
		return scenarios.ListObjects(), 0
	case "bidi-read":
		return scenarios.BidiRead(concurrency), size
	case "bidi-write":
		// bidi-write is handled separately in the run loop to iterate chunk sizes
		return nil, 0
	}
	return nil, 0
}

// scenariosNeedSize returns the sizes to iterate for a given scenario.
// head, list, bidi-read, bidi-write operate on fixed objects — return a single sentinel.
func scenariosNeedSize(scenario string, sizes []int64) []int64 {
	switch scenario {
	case "head", "list", "bidi-read", "bidi-write":
		return []int64{0} // size is not a variable for these scenarios
	default:
		return sizes
	}
}

// concurrencyFor limits concurrency options for scenarios where high concurrency is not meaningful.
func concurrencyFor(scenario string, concurrency []int) []int {
	switch scenario {
	case "list":
		// Listing is typically single-threaded; cap at 4
		var result []int
		for _, c := range concurrency {
			if c <= 4 {
				result = append(result, c)
			}
		}
		if len(result) == 0 {
			return []int{1}
		}
		return result
	default:
		return concurrency
	}
}

func printRunInfo(cfg *config.Config) {
	fmt.Println("=== GCS REST vs gRPC Benchmark ===")
	fmt.Printf("Bucket:      %s\n", cfg.Bucket)
	fmt.Printf("Project:     %s\n", cfg.Project)
	fmt.Printf("Region:      %s\n", cfg.Region)
	fmt.Printf("Scenarios:   %v\n", cfg.Scenarios)
	fmt.Printf("Sizes:       %v\n", cfg.Sizes)
	fmt.Printf("Concurrency: %v\n", cfg.Concurrency)
	fmt.Printf("Iterations:  %d (warmup: %d)\n", cfg.Iterations, cfg.Warmup)
	fmt.Printf("Clients:     %v\n", cfg.Clients)
	fmt.Printf("Output:      %s (%s)\n", cfg.OutputDir, cfg.OutputFormat)

	if info, ok := debug.ReadBuildInfo(); ok {
		fmt.Printf("Go version:  %s\n", info.GoVersion)
		for _, dep := range info.Deps {
			if dep.Path == "cloud.google.com/go/storage" {
				fmt.Printf("GCS client:  %s %s\n", dep.Path, dep.Version)
			}
		}
	}
	fmt.Println()
}
