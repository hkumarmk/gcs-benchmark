package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/storage-benchmark/gcs-benchmark/client"
	"github.com/storage-benchmark/gcs-benchmark/setup"
)

// ReadObject returns a ScenarioFn for full object reads of the given size.
// Each worker reads its own dedicated object key to avoid cross-worker cache effects.
func ReadObject(size int64) ScenarioFn {
	return func(ctx context.Context, c client.StorageClient, workerID int) (time.Duration, int64, error) {
		key := setup.ReadKey(c.TransportName(), workerID, size)
		data, ttfb, err := c.Read(ctx, key)
		if err != nil {
			return 0, 0, fmt.Errorf("read %s: %w", key, err)
		}
		return ttfb, int64(len(data)), nil
	}
}
