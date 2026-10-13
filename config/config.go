package config

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Config holds all benchmark parameters parsed from CLI flags.
type Config struct {
	Bucket       string
	Project      string
	Region       string
	Scenarios    []string
	Sizes        []int64
	Concurrency  []int
	Iterations   int
	Warmup       int
	Clients      []string // "rest", "grpc"
	OutputDir    string
	OutputFormat string // "json", "csv", "both"
	Setup        bool
	Teardown     bool
	IdleBetween  time.Duration
}

func Parse() (*Config, error) {
	c := &Config{}

	flag.StringVar(&c.Bucket, "bucket", "", "GCS bucket name (required)")
	flag.StringVar(&c.Project, "project", "", "GCP project ID (required)")
	flag.StringVar(&c.Region, "region", "us-central1", "Bucket region")

	scenarios := flag.String("scenarios", "read,write,head,list", "Scenarios: read,write,head,list,bidi-read,bidi-write (bidi-* require GCP preview access)")
	sizes := flag.String("sizes", "4k,64k,1m,10m", "Object sizes: 4k,64k,1m,10m")
	concurrency := flag.String("concurrency", "1,4,8,16", "Concurrency levels")
	clients := flag.String("clients", "all", "Clients: rest,rest-pool1,grpc,all")

	flag.IntVar(&c.Iterations, "iterations", 20, "Measurement iterations per worker per scenario/size/concurrency (total = iterations * concurrency)")
	flag.IntVar(&c.Warmup, "warmup", 5, "Warmup iterations to discard")
	flag.StringVar(&c.OutputDir, "output-dir", "./results", "Directory for output files")
	flag.StringVar(&c.OutputFormat, "output-format", "both", "Output format: json,csv,both")
	flag.BoolVar(&c.Setup, "setup", true, "Run setup phase")
	flag.BoolVar(&c.Teardown, "teardown", false, "Run teardown phase (default false — reuse bucket across runs)")
	flag.DurationVar(&c.IdleBetween, "idle-between", 30*time.Second, "Idle time between scenarios")

	flag.Parse()

	if c.Bucket == "" {
		return nil, fmt.Errorf("--bucket is required")
	}
	if c.Project == "" {
		return nil, fmt.Errorf("--project is required")
	}

	var err error
	c.Scenarios, err = parseScenarios(*scenarios)
	if err != nil {
		return nil, err
	}
	c.Sizes, err = parseSizes(*sizes)
	if err != nil {
		return nil, err
	}
	c.Concurrency, err = parseConcurrency(*concurrency)
	if err != nil {
		return nil, err
	}
	c.Clients, err = parseClients(*clients)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func parseScenarios(s string) ([]string, error) {
	all := []string{"read", "write", "head", "list", "bidi-read", "bidi-write"}
	if s == "all" {
		// "all" excludes bidi-* which require GCP private preview access
		return []string{"read", "write", "head", "list"}, nil
	}
	valid := map[string]bool{}
	for _, v := range all {
		valid[v] = true
	}
	var result []string
	for _, tok := range strings.Split(s, ",") {
		tok = strings.TrimSpace(tok)
		if !valid[tok] {
			return nil, fmt.Errorf("unknown scenario %q", tok)
		}
		result = append(result, tok)
	}
	return result, nil
}

func parseSizes(s string) ([]int64, error) {
	units := map[string]int64{"k": 1024, "m": 1024 * 1024, "g": 1024 * 1024 * 1024}
	var result []int64
	for _, tok := range strings.Split(s, ",") {
		tok = strings.TrimSpace(strings.ToLower(tok))
		if len(tok) == 0 {
			continue
		}
		suffix := string(tok[len(tok)-1])
		mult, ok := units[suffix]
		if !ok {
			n, err := strconv.ParseInt(tok, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid size %q", tok)
			}
			result = append(result, n)
			continue
		}
		n, err := strconv.ParseInt(tok[:len(tok)-1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid size %q", tok)
		}
		result = append(result, n*mult)
	}
	return result, nil
}

func parseConcurrency(s string) ([]int, error) {
	var result []int
	for _, tok := range strings.Split(s, ",") {
		tok = strings.TrimSpace(tok)
		n, err := strconv.Atoi(tok)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("invalid concurrency %q", tok)
		}
		result = append(result, n)
	}
	return result, nil
}

func parseClients(s string) ([]string, error) {
	if s == "all" {
		return []string{"rest", "rest-pool1", "grpc"}, nil
	}
	valid := map[string]bool{"rest": true, "rest-pool1": true, "grpc": true}
	var result []string
	for _, tok := range strings.Split(s, ",") {
		tok = strings.TrimSpace(tok)
		if !valid[tok] {
			return nil, fmt.Errorf("unknown client %q", tok)
		}
		result = append(result, tok)
	}
	return result, nil
}
