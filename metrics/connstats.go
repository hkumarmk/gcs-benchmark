package metrics

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/storage-benchmark/gcs-benchmark/client"
)

// ConnStatsEntry records connection behavior for one client run.
type ConnStatsEntry struct {
	Transport        string `json:"transport"`
	NewConnections   int64  `json:"new_connections"`
	ReusedConns      int64  `json:"reused_connections"`
	HTTP2Connections int64  `json:"http2_connections"`
	Note             string `json:"note,omitempty"`
}

// WriteConnStats appends a connection stats entry to connection-stats.json in outputDir.
// Called once per client instance after all scenarios complete.
func WriteConnStats(transport string, stats client.ConnStats, outputDir string) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}

	entry := ConnStatsEntry{
		Transport:        transport,
		NewConnections:   stats.NewConnections,
		ReusedConns:      stats.ReusedConns,
		HTTP2Connections: stats.HTTP2Connections,
	}

	switch transport {
	case "grpc":
		entry.Note = "gRPC always uses one channel; streams are multiplexed, not connections"
	case "rest":
		entry.Note = "default Go HTTP transport; may open multiple TCP connections under concurrency"
	case "rest-pool1":
		entry.Note = "MaxIdleConnsPerHost=1 and MaxConnsPerHost=1; all requests multiplexed on one HTTP/2 connection"
	}

	path := filepath.Join(outputDir, "connection-stats.json")

	// Read existing entries if file exists
	var entries []ConnStatsEntry
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &entries)
	}
	entries = append(entries, entry)

	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write connection-stats.json: %w", err)
	}
	return nil
}
