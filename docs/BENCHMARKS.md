# Benchmarks

These numbers measure the queue, API, cache and worker machinery against the
fake model. They say nothing about any real model's speed. The fake model has a
fixed 50 ms mean latency (plus or minus 20 percent), so the ideal throughput of
a pool of `W` workers is `W / 0.05` items per second, and any gap below that is
overhead from the service.

## Machine

| | |
|---|---|
| Model | `hw.model` Mac17,4 (MacBook Air) |
| CPU | Apple M5, 10 cores (`hw.ncpu` 10) |
| Memory | 16 GiB |
| OS | macOS 27.0.1 |
| Go | go1.27.4 darwin/arm64 |
| Postgres | embedded PostgreSQL 16.9, default settings, `fsync` on, same machine |
| Cache | in-process memory (no Redis in the load test) |

This is a shared laptop. Other heavy jobs were running during the measurements:
the one-minute load average was between 4 and 8 on 10 cores while these ran. The
load generator, API, workers and Postgres all share the machine. Treat the
figures as indicative, not as a controlled benchmark.

## Command

```sh
go run ./cmd/loadtest -workers 4,16,64 -items 1000 -batch-size 100 \
    -model-latency 50ms -dup-ratio 0.8
go run ./cmd/loadtest -workers 16 -rate 150 -duration 10s \
    -model-latency 50ms -dup-ratio 0.8
```

The program starts one process containing the HTTP API (on a loopback listener),
a worker pool and Postgres. It creates a key, submits batches over HTTP, waits
until every item is `succeeded`, then reads latencies from the database.
Between scenarios it truncates the tables.

- **Throughput** is items divided by the time from the first item's creation to
  the last item's completion.
- **Latency** is per item, from the moment its batch was accepted to the moment
  the worker committed the result (`completed_at - created_at`). In burst mode
  every item is submitted at once, so latency includes waiting in the queue
  behind the rest of the burst. That is why burst percentiles are large and
  roughly equal to the total drain time for the slowest items.
- **Cache off**: 1,000 distinct prompts. **Cache on**: the same 1,000 items drawn
  from 199 distinct prompts (an 80 percent repeat rate), with the in-memory
  cache and in-process request collapsing enabled. Hit rates in real workloads
  will differ; this shows the mechanism, not a prediction.
- Submissions use 8 concurrent HTTP clients, 100 items per batch.

## Results

### Burst: 1,000 items submitted at once

| workers | cache | model calls | seconds | items/s | ideal items/s | p50 ms | p95 ms | p99 ms |
|---:|:---:|---:|---:|---:|---:|---:|---:|---:|
| 4 | off | 1000 | 12.92 | 77 | 80 | 6487 | 12262 | 12783 |
| 4 | on | 199 | 3.10 | 323 | n/a | 2733 | 3057 | 3086 |
| 16 | off | 1000 | 3.40 | 294 | 320 | 1777 | 3220 | 3344 |
| 16 | on | 199 | 0.82 | 1215 | n/a | 689 | 801 | 813 |
| 64 | off | 1000 | 1.26 | 792 | 1280 | 734 | 1203 | 1238 |
| 64 | on | 199 | 0.43 | 2323 | n/a | 264 | 407 | 420 |

### Paced: 150 items/s offered for 10 seconds, 16 workers

Offered load is below capacity (16 workers can do about 320 per second), so
queue wait stays small and the latencies show service time plus queueing noise.

| workers | cache | items | model calls | items/s | p50 ms | p95 ms | p99 ms |
|---:|:---:|---:|---:|---:|---:|---:|---:|
| 16 | off | 1500 | 1500 | 150 | 61 | 79 | 85 |
| 16 | on | 1500 | 199 | 150 | 18 | 66 | 79 |

## Reading the numbers

- With 4 and 16 workers the service reaches 96 and 92 percent of the ideal rate,
  so overhead is small when workers are the limit. The first few hundred
  milliseconds of each run include lease polling and the submission of the burst.
- With 64 workers throughput is 62 percent of ideal. At that point the limit is
  no longer model latency: it is the cost of leasing in small groups, one
  transaction per completion that also updates the batch row (10 batches are in
  flight, so completions contend), and everything sharing one laptop with
  `fsync` on. DESIGN.md lists the counter contention as the first thing to fix
  if this mattered.
- Paced latency with the cache off is the 50 ms model time plus about 10 ms of
  queueing, claiming and commit.
- The cache cut model calls from 1,000 to 199 and raised throughput 3 to 4
  times in burst mode, because duplicate prompts no longer wait for a worker slot
  and a model call.
- Items per second here are not a limit of Postgres; they are what this single
  process, on a loaded laptop, did against a 50 ms model.

## Not measured

Redis round trips (the cache here is in memory), a networked Postgres, real
model latency distributions, large prompts and outputs, and multi-process
workers. The tests exercise the Redis and multi-process paths for correctness
but not speed.
