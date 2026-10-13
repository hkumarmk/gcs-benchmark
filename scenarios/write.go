package scenarios

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/storage-benchmark/gcs-benchmark/client"
	"github.com/storage-benchmark/gcs-benchmark/setup"
)

// writeData holds pre-generated random bytes per size, keyed by size in bytes.
// Generated once with a fixed seed — reused across all write iterations.
var writeData = func() map[int64][]byte {
	rng := rand.New(rand.NewSource(42))
	m := make(map[int64][]byte)
	for _, size := range setup.ObjectSizes {
		buf := make([]byte, size)
		rng.Read(buf)
		m[size] = buf
	}
	return m
}()

// WriteObject returns a ScenarioFn for full object writes of the given size.
// Each iteration writes to a unique key to avoid GCS deduplication effects.
func WriteObject(size int64) ScenarioFn {
	data := writeData[size]
	return func(ctx context.Context, c client.StorageClient) (time.Duration, int64, error) {
		key := fmt.Sprintf("%s%s/%s/%d", setup.WritePrefix, sizeLabel(size), c.TransportName(), time.Now().UnixNano())
		if err := c.Write(ctx, key, data); err != nil {
			return 0, 0, fmt.Errorf("write %s: %w", key, err)
		}
		return 0, size, nil
	}
}

func sizeLabel(size int64) string {
	switch {
	case size < 1024*1024:
		return fmt.Sprintf("%dk", size/1024)
	default:
		return fmt.Sprintf("%dm", size/(1024*1024))
	}
}
