# GCS REST vs gRPC Benchmark

Measures the best achievable performance of a correctly configured GCS REST client against a correctly configured GCS gRPC client. Both clients use the official `google-cloud-go/storage` library — transport is the only variable.

See [`docs/gcs-benchmark-detailed-plan.md`](../docs/gcs-benchmark-detailed-plan.md) for the full benchmark design, methodology, and interpretation notes.
