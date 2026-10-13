# GCS REST vs gRPC Benchmark: Detailed Plan

## Context and Purpose

### Background

At a Google Cloud conference, Google's engineering team presented performance improvements of 20–30% for GCS workloads by moving from the REST API to a new gRPC-based API. The improvement was not from gRPC alone — it came from a specific combination of three factors:

**1. New streaming API semantics**
Google introduced `BidiReadObject` and `BidiWriteObject` — bidirectional streaming RPCs. `BidiReadObject` allows a client to send multiple range read requests over a single long-lived stream without waiting for each response, and receive data back as it becomes available. This eliminates the round-trip cost between each range read — directly relevant to ML training workloads where a DataLoader prefetches multiple data chunks ahead of GPU consumption. The demonstrated use case was random range reads from large dataset files, not general-purpose object storage operations.

**2. GFE bypass**
GCS REST requests route through Google's Frontend (GFE) — a global edge proxy handling TLS termination, load balancing, and HTTP protocol processing. GCS gRPC requests bypass GFE entirely and land directly at the GCS internal frontend. This is a real topology advantage: one fewer proxy, reduced per-request overhead on every operation regardless of object size or access pattern.

**3. GCS-internal optimisations**
The gRPC path in GCS may have backend optimisations not present in the REST path. These are not observable or reproducible externally.

### Why We Are Doing This Benchmark

We are considering building an open source gRPC-based object storage gateway — a significant engineering undertaking. This is not a microservice; it is critical storage infrastructure requiring auth, consistency, durability guarantees, operational tooling, and long-term maintenance. The decision to build it needs to be grounded in evidence, not in a conference claim made for a specific system under specific conditions.

This benchmark uses GCS as a controlled environment — the same system where the 30% was demonstrated — to understand how much of that improvement holds up when:

- The REST client is properly configured with HTTP/2, connection pooling, and pipelined range reads — not a naive baseline
- Both sides use the best available API for each scenario
- The GFE bypass component is estimated and separated from protocol and API design contributions

The results will tell us whether there is enough genuine performance benefit — beyond GCS-specific topology advantages that won't transfer to other storage systems — to justify the investment in building a gateway.

**What this benchmark is not:**

- Not a comparison of HTTP/1.1 vs gRPC — both sides use HTTP/2
- Not a comparison of naive REST vs optimised gRPC — both sides get best-effort configuration
- Not a Ceph or any other storage system benchmark — GCS results cannot be directly applied elsewhere

---

## Implementation Language

**Go.**

- Goroutines give precise concurrency control without thread pool tuning or GIL interference
- `google-cloud-go/storage` supports both HTTP/2 REST and native gRPC transport via a single client option switch
- Reproducible, publishable as open source
- No runtime overhead distorting concurrency measurements

---

## Client Configuration

Both clients use the official `google-cloud-go/storage` library — the same library, switching between REST and gRPC transport via a single client option. This ensures both sides benefit equally from Google's own client optimisations and neither is disadvantaged by a third-party or hand-rolled implementation.

### REST Client

- Uses `google-cloud-go/storage` in default HTTP transport mode — HTTP/2 via ALPN negotiation
- Connection pool sized to match the concurrency level under test
- OAuth2 credentials via Application Default Credentials, token reused across all requests in a run
- No additional middleware or interceptors in the hot path

### gRPC Client

- Uses `google-cloud-go/storage` with `option.WithGRPCConnectionPool(1)` — single channel, HTTP/2 multiplexing handles concurrent streams
- Keepalive parameters set explicitly to maintain long-lived connections across iterations
- Flow control window sized for large object transfers (at least 16 MB)
- Same Application Default Credentials as REST client

---

## Test Objects

Upload all objects once at the start. Run all benchmark iterations against pre-uploaded objects. Delete everything in a single pass at the end.

Do not delete and recreate objects between runs — this would introduce GCS cold-start effects (metadata cache, routing) that are not part of what we are measuring.

| Size | Rationale |
|------|-----------|
| 4 KB | Protocol overhead dominates; connection cost is proportionally large |
| 64 KB | HTTP/2 flow control window behaviour starts to matter; common ML metadata size |
| 1 MB | Typical embedding or feature file |
| 10 MB | Moderate checkpoint shard |
| 100 MB | Large transfer ceiling; protocol overhead should approach zero |

---

## Concurrency Ladder

Every scenario runs at all four concurrency levels:

| Workers | Rationale |
|---------|-----------|
| 1 | Baseline and sanity check; expected near-zero protocol difference |
| 4 | Low-end DataLoader `num_workers` |
| 16 | Realistic ML training prefetch concurrency |
| 64 | Stress; reveals saturation and degradation behaviour |

---

## Metrics Per Run

| Metric | Applies To |
|--------|-----------|
| P50 latency | All scenarios |
| P95 latency | All scenarios |
| P99 latency | All scenarios |
| Throughput (MB/s) | Read and write scenarios |
| Time to first byte | Read scenarios — isolates connection/stream setup cost from transfer time |
| Operations/sec | Small object scenarios (4 KB, 64 KB at high concurrency) |
| Client CPU (%) | All scenarios — gRPC protobuf overhead vs REST JSON/header overhead |

Each scenario runs a minimum of 100 iterations per concurrency level to produce stable percentiles. Discard the first 5 iterations as warm-up.

---

## Section 1: Standard Operations

Symmetric comparison. Same operation semantics on both sides.

### 1a. Full Object Read

```
REST: GET https://storage.googleapis.com/bucket/object
gRPC: ReadObject(bucket, object)
```

Object sizes: 4 KB, 64 KB, 1 MB, 10 MB, 100 MB  
Concurrency: 1, 4, 16, 64  
Metrics: P50/P95/P99 latency, throughput, time to first byte

### 1b. Full Object Write

```
REST: PUT https://storage.googleapis.com/upload/storage/v1/b/bucket/o
gRPC: WriteObject(bucket, object, data)
```

Object sizes: 4 KB, 64 KB, 1 MB, 10 MB, 100 MB  
Concurrency: 1, 4, 16, 64  
Metrics: P50/P95/P99 latency, throughput

### 1c. Metadata Read (HEAD)

```
REST: GET with fields=name,size,contentType,updated
gRPC: GetObject(bucket, object) — metadata only
```

No payload transfer. Measures pure request/response overhead.  
Concurrency: 1, 4, 16, 64  
Metrics: P50/P95/P99 latency, requests/sec

### 1d. Object Listing

```
REST: GET https://storage.googleapis.com/storage/v1/b/bucket/o?prefix=...
gRPC: ListObjects(bucket, prefix)
```

Bucket pre-populated with 10,000 objects under a common prefix.  
Measure time to list all 10,000.  
Concurrency: 1, 4 (listing is usually single-threaded; high concurrency less relevant)  
Metrics: Total completion time, objects/sec

---

## Section 2: Streaming Operations

**Asymmetric by design.** Both sides use their best available API. The delta measures API design advantage.

### 2a. BidiReadObject vs Pipelined HTTP Range Reads

Scenario: Read N non-overlapping 1 MB ranges from a single 100 MB object.

```
REST:   N goroutines each issuing an HTTP Range request concurrently, all in-flight simultaneously
        over a shared HTTP/2 connection pool with keepalive and session reuse
gRPC:   Single BidiReadObject stream with N outstanding ReadObjectRequest messages, no wait between sends
```

N matches concurrency ladder: 4, 16, 64 outstanding requests  
Metrics: Total completion time, P50/P95/P99 per-range latency, throughput, time to first byte of first range

This scenario is the closest analogue to ML DataLoader prefetch behaviour — multiple range reads issued ahead of consumption without waiting for each to complete.

Both sides are fully pipelined. The REST client is not handicapped with sequential reads. The remaining structural differences are:
- Per-request overhead: HTTP headers vs small protobuf message
- Whether GCS routes REST and gRPC endpoints through identical internal paths
- Flow control granularity at the HTTP/2 stream level

This is the scenario most likely to isolate true protocol/API advantage from benchmark methodology. If gRPC still wins here with both sides properly pipelined, the finding is meaningful. If the delta disappears, that is equally meaningful.

### 2b. BidiWriteObject vs Resumable Upload

Scenario: Write a single 100 MB object as a stream.

```
REST:   GCS resumable upload — POST to initiate session, stream body in chunks, receive final response
gRPC:   BidiWriteObject — stream WriteObjectRequest messages, receive progressive acknowledgements
```

Chunk sizes for REST: 8 MB (GCS recommended minimum for resumable), 1 MB  
Concurrency: 1, 4, 16 (streaming writes are typically less parallel than reads)  
Metrics: P50/P95/P99 end-to-end latency, throughput, time to first acknowledgement

---

## Test Environment

- Single GCP project, single bucket, `us-central1`
- Benchmark client runs on a GCP VM (`n2-standard-8`) in the same region — eliminates egress cost and cross-region variance
- All runs on the same VM instance, same bucket, same objects
- Sequential scenario execution with 30-second idle between scenarios to drain connection state
- System clock synchronisation verified before runs (NTP)
- No other workload on the VM during benchmark

**Cost controls:**
- VM runs only during benchmark execution (preemptible/spot instance acceptable)
- All objects deleted in single pass at end
- Estimated total cost: under $10 including VM time

---

## Run Structure

```
startup:
  - create bucket
  - upload all test objects (one of each size)
  - create listing prefix with 10,000 objects

for each scenario:
  for each object size (where applicable):
    for each concurrency level:
      warm-up: 5 iterations (discarded)
      measurement: 100 iterations
      record: all metrics to structured output (JSON)

teardown:
  - delete all objects
  - delete bucket
  - stop VM
```

---

## Output Format

Structured JSON per iteration, aggregated to CSV/table for reporting.

| Scenario | Size | Concurrency | Client | P50 (ms) | P95 (ms) | P99 (ms) | Throughput (MB/s) | TTFB (ms) | CPU (%) |
|----------|------|-------------|--------|----------|----------|----------|-------------------|-----------|---------|
| Read | 4 KB | 1 | REST | | | | | | |
| Read | 4 KB | 1 | gRPC | | | | | | |
| Read | 4 KB | 16 | REST | | | | | | |
| Read | 4 KB | 16 | gRPC | | | | | | |
| ... | | | | | | | | | |
| BidiRead | 100 MB | 16 ranges | REST | | | | | | |
| BidiRead | 100 MB | 16 ranges | gRPC | | | | | | |

---

## Interpretation Notes

Results should be read with the following in mind:

- Any improvement under concurrency=1 is protocol/framing overhead only
- Improvement that grows with concurrency suggests multiplexing advantage
- Improvement that appears only in Section 2 suggests API design advantage, not protocol advantage
- Client CPU delta is relevant if this gateway will co-locate with training jobs
- Time to first byte is the metric most relevant to GPU stall — a faster TTFB means the DataLoader can start feeding the GPU sooner

---

## Confounds and Interpretation Limits

### The GFE Bypass Confound

This is the most important caveat for anyone using these results to justify a Ceph gateway.

GCS REST and gRPC requests follow different network paths:

```
REST:   Client → GFE → GCS Frontend → Colossus storage nodes
gRPC:   Client → GCS Frontend → Colossus storage nodes
```

GFE (Google Frontend) is a global edge layer handling TLS termination, HTTP protocol parsing, load balancing, and request routing. gRPC requests bypass it entirely and land directly at the GCS frontend. This is a genuine topology advantage — one fewer proxy, one fewer TLS handshake, reduced latency on every request.

However:
- The GCS frontend still handles auth, quota, routing, and the Colossus RPC regardless of path. GFE bypass saves edge overhead only, not storage-side work.
- Any improvement measured in this benchmark is a mix of three things: GFE bypass, per-request framing overhead reduction, and any internal optimisations GCS applies specifically to the gRPC path.
- These three components cannot be separated from outside Google's network.

**Why this does not transfer to Ceph:**

GCS gRPC removes a hop that exists in the GCS REST path — GFE. There is no equivalent hop in the Ceph architecture to remove:

```
GCS REST:   Client → GFE → GCS Frontend → Colossus
GCS gRPC:   Client → GCS Frontend → Colossus          ← GFE removed

Ceph now:   Client → RGW (librados) → RADOS
Ceph gRPC:  Client → gRPC gateway (librados) → RADOS  ← nothing removed, RGW replaced
```

The proposed Ceph gateway replaces RGW at the same position in the stack. Both use librados. The hop count does not change. There is no GFE equivalent sitting in front of RGW that gets eliminated.

This means the GCS benchmark result contains a topology advantage that the Ceph architecture cannot replicate. Any improvement measured on GCS gRPC is partly GFE bypass — and that component simply does not exist in the Ceph comparison.

Ceph gateway gains must come from different mechanisms entirely — more efficient librados usage, parallel async stripe reads, reduced RGW processing overhead — and must be measured independently against a RGW baseline.

**Conclusion:** Do not use GCS gRPC benchmark numbers to project Ceph gateway gains. The topologies are not comparable.

---

## What This Benchmark Cannot Answer

- Whether the same results would appear on Ceph with a librados-backed gateway
- Whether application-level training job time improves (requires end-to-end ML benchmark)
- Whether improvements are due to GCS internal implementation differences between REST and gRPC endpoints rather than protocol differences
- Whether a tuned Ceph + RGW baseline would close the gap observed here
- What fraction of the measured improvement is GFE bypass vs protocol vs API design
