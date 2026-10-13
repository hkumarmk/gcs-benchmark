# GCS REST vs gRPC Benchmark

Measures the best achievable performance of a correctly configured GCS REST client against a correctly configured GCS gRPC client. Both clients use the official `google-cloud-go/storage` library — transport is the only variable.

See [`docs/gcs-benchmark-detailed-plan.md`](docs/gcs-benchmark-detailed-plan.md) for the full benchmark design, methodology, and interpretation notes.

---

## Prerequisites

- Go 1.21+
- A GCP project with billing enabled and the Cloud Storage API enabled
- A GCP account with `roles/storage.admin` (or `roles/owner`) on the project
- `gcloud` CLI installed and authenticated

**Run from outside GCP** — see the note in [Running](#running).

---

## Setup: gcloud CLI and Go

### Install gcloud CLI

```bash
# macOS with Homebrew
brew install --cask google-cloud-sdk

# Other platforms: https://cloud.google.com/sdk/docs/install
```

### Install Go

```bash
# macOS with Homebrew
brew install go

# Other platforms: https://go.dev/dl/
```

### Authenticate

```bash
# Login to gcloud
gcloud auth login

# Set application default credentials used by the benchmark
gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/cloud-platform

# Set your project
gcloud config set project YOUR_PROJECT_ID

# Verify
gcloud auth application-default print-access-token
gcloud config list
```

The benchmark uses Application Default Credentials. No service account key file needed.

---

## Build

```bash
cd benchmark
go build -o gcs-benchmark .
```

---

## Running

### Full benchmark (all scenarios, both transports)

```bash
./gcs-benchmark \
  --bucket my-bench-bucket \
  --project my-gcp-project
```

This will:
1. Create the bucket in `us-central1`
2. Upload all fixture objects
3. Run all scenarios at all concurrency levels for both REST and gRPC
4. Write results to `./results/`
5. Delete all objects and the bucket

**Run from outside GCP — your laptop or corporate network, not a GCP VM.**

GCS REST requests from outside GCP route through Google's Frontend (GFE). GCS gRPC requests bypass GFE. This topology difference is part of what this benchmark is measuring. Running from inside GCP may change or eliminate the GFE path for both transports, which would invalidate the comparison — you would no longer be measuring the conditions under which Google's 30% claim was made.

Egress cost from outside GCP is small for this dataset — see [Cost](#cost) below.

### Run specific scenarios only

```bash
./gcs-benchmark \
  --bucket my-bench-bucket \
  --project my-gcp-project \
  --scenarios read,head
```

### Run a single scenario at specific sizes and concurrency

```bash
./gcs-benchmark \
  --bucket my-bench-bucket \
  --project my-gcp-project \
  --scenarios bidi-read \
  --sizes 100m \
  --concurrency 4,16,64
```

### Re-run without repeating setup/teardown

Useful when iterating on a specific scenario — fixtures stay in the bucket between runs:

```bash
# First run: setup only
./gcs-benchmark \
  --bucket my-bench-bucket \
  --project my-gcp-project \
  --scenarios read \
  --teardown=false

# Subsequent runs: skip setup and teardown
./gcs-benchmark \
  --bucket my-bench-bucket \
  --project my-gcp-project \
  --scenarios read \
  --setup=false \
  --teardown=false

# Final run: teardown
./gcs-benchmark \
  --bucket my-bench-bucket \
  --project my-gcp-project \
  --scenarios read \
  --setup=false
```

### Run gRPC only

```bash
./gcs-benchmark \
  --bucket my-bench-bucket \
  --project my-gcp-project \
  --clients grpc
```

---

## All Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--bucket` | required | GCS bucket name |
| `--project` | required | GCP project ID |
| `--region` | `us-central1` | Bucket region |
| `--scenarios` | `all` | `read,write,head,list,bidi-read,bidi-write,all` |
| `--sizes` | `4k,64k,1m,10m,100m` | Object sizes to test |
| `--concurrency` | `1,4,16,64` | Concurrency levels |
| `--iterations` | `20` | Measurement iterations per combination |
| `--warmup` | `5` | Warmup iterations to discard |
| `--clients` | `all` | `rest,grpc,all` |
| `--output-dir` | `./results` | Directory for output files |
| `--output-format` | `both` | `json,csv,both` |
| `--setup` | `true` | Run setup phase |
| `--teardown` | `true` | Run teardown phase |
| `--idle-between` | `30s` | Idle time between scenarios |

---

## Scenarios

| Scenario | What it measures |
|----------|-----------------|
| `read` | Full object read — all sizes, all concurrency levels |
| `write` | Full object write — all sizes, all concurrency levels |
| `head` | Metadata read only, no payload — pure request/response overhead |
| `list` | List 10,000 objects — pagination overhead |
| `bidi-read` | N pipelined range reads from 100MB object — REST uses N concurrent HTTP requests, gRPC uses `BidiReadObject` via `NewMultiRangeDownloader` |
| `bidi-write` | 100MB streaming write — REST uses resumable upload, gRPC uses `BidiWriteObject`; tested at 1MB and 8MB chunk sizes |

---

## Output

Results are written to `--output-dir` (default `./results/`):

- One JSON file per `(scenario, transport, size, concurrency)` combination
- A single `summary.csv` with all results in one table
- A formatted comparison table printed to stdout with delta rows showing REST→gRPC improvement

Example stdout output:

```
Scenario             Size     Concurrency  Client  P50(ms)  P95(ms)  P99(ms)   MB/s  OPS/s  CPU%
──────────────────────────────────────────────────────────────────────────────────────────────────
read                 4KB      1            rest      12.3     18.7     24.1     0.32   81.3   2.1
read                 4KB      1            grpc      10.1     14.2     18.9     0.39   98.9   2.4
                              1            DELTA    -18.0%   -24.1%   -21.6%

read                 4KB      64           rest       3.1      6.2      9.4    82.1  322.1   18.3
read                 4KB      64           grpc       2.8      5.1      7.8    91.4  356.2   21.0
                              64           DELTA     -9.7%   -17.7%   -17.0%
```

Negative delta = gRPC faster (lower latency). Positive delta = REST faster.

---

## Cost

Running from outside GCP (recommended):
- Egress: at default 20 iterations, total egress across all scenarios is roughly 30-35 GB ≈ $2-3
- Storage: ~200MB of fixture objects for less than one day ≈ negligible
- Operations: GCS Class A/B operation charges ≈ negligible

Estimated total cost: **under $5.**

If results look noisy or you want tighter percentiles, increase with `--iterations 50` (≈$6-7) or `--iterations 100` (≈$12-15). 20 iterations is sufficient for a directional benchmark — the signal we're looking for (10-30% difference) is large enough to show clearly at this sample size.

The benchmark deletes all objects and the bucket on teardown. Use `--teardown=false` only if you intend to clean up manually — orphaned objects will incur storage charges.
