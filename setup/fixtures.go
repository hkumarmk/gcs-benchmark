package setup

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

const (
	ReadPrefix    = "read-bench/"
	RangePrefix   = "range-read-bench/"
	ListingPrefix = "listing-bench/"
	WritePrefix   = "write-bench/"
	ListingCount  = 1000
	MaxWorkers    = 16 // number of per-worker object copies staged per transport per size
)

// ObjectSizes are the fixture sizes uploaded during setup.
var ObjectSizes = []int64{
	4 * 1024,
	64 * 1024,
	1 * 1024 * 1024,
	10 * 1024 * 1024,
}

// ReadKey returns the object key for a given transport, worker ID, and size.
// Each worker has its own dedicated object to avoid cross-worker cache effects.
func ReadKey(transport string, workerID int, size int64) string {
	return fmt.Sprintf("%s%s/worker-%d/%s", ReadPrefix, transport, workerID, SizeKey(size))
}

// Setup creates the bucket if needed, uploads source fixtures, copies them per transport/worker,
// and populates the listing prefix.
func Setup(ctx context.Context, sc *storage.Client, project, bucket, region string) error {
	if err := ensureBucket(ctx, sc, project, bucket, region); err != nil {
		return fmt.Errorf("ensure bucket: %w", err)
	}

	rng := rand.New(rand.NewSource(42))

	// Upload one source object per size
	fmt.Println("Uploading source fixtures...")
	for _, size := range ObjectSizes {
		data := make([]byte, size)
		rng.Read(data)
		key := fmt.Sprintf("%ssrc/%s", ReadPrefix, sizeKey(size))
		if err := upload(ctx, sc, bucket, key, data); err != nil {
			return fmt.Errorf("upload %s: %w", key, err)
		}
		fmt.Printf("  uploaded %s (%d bytes)\n", key, size)
	}

	// Server-side copy source objects to per-transport per-worker keys
	fmt.Printf("Copying fixtures for %d workers x 3 transports...\n", MaxWorkers)
	for _, transport := range []string{"rest", "rest-pool1", "grpc"} {
		for workerID := 0; workerID < MaxWorkers; workerID++ {
			for _, size := range ObjectSizes {
				src := fmt.Sprintf("%ssrc/%s", ReadPrefix, sizeKey(size))
				dst := ReadKey(transport, workerID, size)
				if err := serverCopy(ctx, sc, bucket, src, dst); err != nil {
					return fmt.Errorf("copy %s -> %s: %w", src, dst, err)
				}
			}
		}
	}
	fmt.Printf("  copied %d objects\n", MaxWorkers*3*len(ObjectSizes))

	fmt.Printf("Uploading %d listing fixtures (concurrent)...\n", ListingCount)
	if err := uploadListing(ctx, sc, bucket); err != nil {
		return fmt.Errorf("upload listing fixtures: %w", err)
	}

	fmt.Println("Setup complete.")
	return nil
}

// Teardown deletes all benchmark objects and the bucket.
func Teardown(ctx context.Context, sc *storage.Client, bucket string) error {
	prefixes := []string{ReadPrefix, RangePrefix, ListingPrefix, WritePrefix}
	for _, prefix := range prefixes {
		fmt.Printf("Deleting objects under %s...\n", prefix)
		if err := deletePrefix(ctx, sc, bucket, prefix); err != nil {
			fmt.Printf("  warning: %v\n", err)
		}
	}

	fmt.Printf("Deleting bucket %s...\n", bucket)
	if err := sc.Bucket(bucket).Delete(ctx); err != nil {
		return fmt.Errorf("delete bucket: %w", err)
	}

	fmt.Println("Teardown complete.")
	return nil
}

// SizeKey returns the object name suffix for a given size.
func SizeKey(size int64) string { return sizeKey(size) }

func sizeKey(size int64) string {
	switch size {
	case 4 * 1024:
		return "4k"
	case 64 * 1024:
		return "64k"
	case 1 * 1024 * 1024:
		return "1m"
	case 10 * 1024 * 1024:
		return "10m"
	default:
		return fmt.Sprintf("%db", size)
	}
}

func ensureBucket(ctx context.Context, sc *storage.Client, project, bucket, region string) error {
	b := sc.Bucket(bucket)
	_, err := b.Attrs(ctx)
	if err == nil {
		fmt.Printf("Bucket %s already exists.\n", bucket)
		return nil
	}
	var apiErr *googleapi.Error
	if !errors.Is(err, storage.ErrBucketNotExist) {
		if !errors.As(err, &apiErr) || apiErr.Code != 404 {
			return err
		}
	}
	fmt.Printf("Creating bucket %s in %s...\n", bucket, region)
	return b.Create(ctx, project, &storage.BucketAttrs{Location: region})
}

func upload(ctx context.Context, sc *storage.Client, bucket, key string, data []byte) error {
	w := sc.Bucket(bucket).Object(key).NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

func serverCopy(ctx context.Context, sc *storage.Client, bucket, src, dst string) error {
	_, err := sc.Bucket(bucket).Object(dst).CopierFrom(sc.Bucket(bucket).Object(src)).Run(ctx)
	return err
}

func uploadListing(ctx context.Context, sc *storage.Client, bucket string) error {
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	errc := make(chan error, ListingCount)
	payload := []byte("x")

	for i := 0; i < ListingCount; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(n int) {
			defer wg.Done()
			defer func() { <-sem }()
			key := fmt.Sprintf("%sobj-%04d", ListingPrefix, n)
			if err := upload(ctx, sc, bucket, key, payload); err != nil {
				errc <- err
			}
		}(i)
	}

	wg.Wait()
	close(errc)

	for err := range errc {
		if err != nil {
			return err
		}
	}
	fmt.Printf("  uploaded %d listing objects\n", ListingCount)
	return nil
}

func deletePrefix(ctx context.Context, sc *storage.Client, bucket, prefix string) error {
	it := sc.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if err := sc.Bucket(bucket).Object(attrs.Name).Delete(ctx); err != nil {
			return err
		}
	}
	return nil
}
