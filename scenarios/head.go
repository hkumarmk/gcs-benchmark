package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/storage-benchmark/gcs-benchmark/client"
	"github.com/storage-benchmark/gcs-benchmark/setup"
)

// HeadObject returns a ScenarioFn for metadata-only reads.
// Uses the 4KB fixture — no payload, measures pure request/response overhead.
func HeadObject() ScenarioFn {
	key := setup.ReadPrefix + "src/4k"
	return func(ctx context.Context, c client.StorageClient, workerID int) (time.Duration, int64, error) {
		_, err := c.Head(ctx, key)
		if err != nil {
			return 0, 0, fmt.Errorf("head %s: %w", key, err)
		}
		return 0, 0, nil
	}
}
