package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/storage-benchmark/gcs-benchmark/client"
	"github.com/storage-benchmark/gcs-benchmark/setup"
)

// ListObjects returns a ScenarioFn that lists all objects under the listing prefix.
// Measures time to iterate all 10,000 objects including pagination.
func ListObjects() ScenarioFn {
	return func(ctx context.Context, c client.StorageClient) (time.Duration, int64, error) {
		count, err := c.List(ctx, setup.ListingPrefix)
		if err != nil {
			return 0, 0, fmt.Errorf("list: %w", err)
		}
		if count < setup.ListingCount {
			return 0, 0, fmt.Errorf("expected %d objects, got %d", setup.ListingCount, count)
		}
		return 0, 0, nil
	}
}
