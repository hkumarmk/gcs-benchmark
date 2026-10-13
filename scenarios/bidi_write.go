package scenarios

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/storage-benchmark/gcs-benchmark/client"
	"github.com/storage-benchmark/gcs-benchmark/setup"
)

var write100m = func() []byte {
	buf := make([]byte, 100*1024*1024)
	rand.New(rand.NewSource(42)).Read(buf)
	return buf
}()

// ChunkSizes are the chunk sizes tested for streaming writes.
// 1MB and 8MB — 8MB is GCS recommended minimum for resumable uploads.
var ChunkSizes = []int{
	1 * 1024 * 1024,
	8 * 1024 * 1024,
}

// BidiWrite returns a ScenarioFn for streaming writes of the 100MB fixture.
// chunkSize controls how data is split into stream messages.
//
// REST:  GCS resumable upload — POST to initiate session, stream body in chunks.
// gRPC:  BidiWriteObject — stream via gRPC transport (WithAppendableUploads).
//
// Requires BidiWriteObject private preview access — use --scenarios bidi-write explicitly.
func BidiWrite(chunkSize int) ScenarioFn {
	return func(ctx context.Context, c client.StorageClient, workerID int) (time.Duration, int64, error) {
		key := fmt.Sprintf("%s100m/%s/chunk%d/w%d/%d",
			setup.WritePrefix, c.TransportName(), chunkSize/1024, workerID, time.Now().UnixNano())
		firstAck, err := c.WriteStreaming(ctx, key, write100m, chunkSize)
		if err != nil {
			return 0, 0, fmt.Errorf("streaming write: %w", err)
		}
		return firstAck, int64(len(write100m)), nil
	}
}
