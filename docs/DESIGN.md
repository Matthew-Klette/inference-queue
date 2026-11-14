# Design

This document explains the choices behind inference-queue, how each failure is
handled, and what guarantees the system does and does not give.

## Shape of the problem

A batch inference job is many independent items. Each item takes between a few
hundred milliseconds and a few minutes, depends on a slow and sometimes
unreliable model backend, and costs real money or GPU time when repeated. The
service therefore cares about three things more than raw queue throughput:

1. Never lose accepted work, even if a worker or the database connection dies.
2. Do not repeat expensive model calls when it can be avoided.
3. Let operators see and repair what is stuck.

## Why Postgres with SKIP LOCKED instead of Kafka

The queue is a table of items. Workers claim rows with:

```sql
SELECT id FROM items
WHERE (status = 'queued' AND run_after <= now())
   OR (status = 'leased' AND lease_expires_at < now() AND attempts < max_attempts)
ORDER BY run_after, id
LIMIT $n
FOR UPDATE SKIP LOCKED
```

and update the claimed rows in the same statement. `SKIP LOCKED` means
concurrent workers never block on each other and never receive the same row.

Reasons this fits here:

- **Transactional enqueue.** A batch and all of its items are inserted in one
  transaction, together with the idempotency record. There is no window in which
  the batch exists but its items do not, and no dual write to reconcile.
- **Transactional completion.** Recording a result, bumping the batch counters,
  and writing the `batch.completed` outbox row are one transaction.
- **The data is the state.** Status, attempts, backoff time, lease owner, last
  error and result are columns. "Why is this item stuck" is a SQL query, not a
  consumer-group offset investigation.
- **Per-item semantics are not stream semantics.** Retry with a per-item delay,
  dead-lettering after N attempts, replaying only the failed items of one batch,
  and returning progress for one batch are natural for rows. In a log they need
  extra topics, a delay mechanism, and a store to answer progress queries.
- **Scale is modest.** The measured throughput of this implementation on a laptop
  is in BENCHMARKS.md. Model calls are slow, so the queue is rarely the
  bottleneck: 1,000 items per second of 50 ms calls needs 50 concurrent calls.
  Postgres handles thousands of claims per second on one node.
- **One thing to operate.** The service needs Postgres anyway for keys, batches
  and results.

### When I would switch, or add Kafka

- Sustained enqueue or completion rates beyond what one Postgres primary handles
  comfortably, in the low tens of thousands of rows per second with this write
  pattern, or table churn that autovacuum cannot keep up with.
- Several independent consumers of the same item stream (billing, analytics,
  training-data capture, audit) that would otherwise poll the database.
- Strict per-key ordering requirements across items.
- Fan-in from many producers where the queue must decouple them from Postgres
  availability.

Even then the first move would be partitioning the items table by time and
keeping Postgres as the system of record, publishing completed-item events from
the outbox to Kafka. The outbox already exists for webhooks, so that is a new
publisher rather than a redesign. Airflow-style scheduling is a different layer;
a DAG run would submit batches through this API.

## Data model

- `api_keys`: sha256 of the key, scopes, rate limit settings, `revoked_at`.
- `batches`: owner, model, canonical params, counters (`total`, `succeeded`,
  `failed`, `scored`, `matched`), status, optional webhook URL, idempotency key
  and request hash. A partial unique index on `(api_key_id, idempotency_key)`
  enforces idempotency.
- `items`: one row per prompt. `status` is `queued`, `leased`, `succeeded` or
  `dead`. Partial indexes cover only the rows a worker can still claim, so the
  index stays small as completed rows accumulate.
- `outbox`: events waiting for delivery, with attempts, `next_attempt_at`,
  `locked_until`, `delivered_at` and `dead`.

Batch status is `queued`, `running`, `completed` or `completed_with_errors`.
Counters live on the batch row and are updated in the same transaction as the
item, so progress reads are a single primary-key lookup.

## Item lifecycle

```
queued --lease--> leased --complete--> succeeded
   ^                 |
   |   fail, retry   +--fail permanent / attempts exhausted--> dead
   +-----------------+
   |   lease expired (attempts left): reclaimed by Lease
   +   shutdown / breaker open: Requeue or Release, attempt refunded
```

- `Lease` increments `attempts` and sets `lease_owner` and `lease_expires_at`.
- `Complete` and `Fail` are fenced: `WHERE id = $1 AND status = 'leased' AND
  lease_owner = $2 AND attempts = $3`. If zero rows change, the worker has lost
  the lease and returns `ErrLeaseLost` without touching the batch counters.
- A retryable failure sets `run_after = now() + backoff`. Backoff is
  `Backoff(attempt, base, max)`: the ceiling doubles from `base` up to `max`, and
  the delay is half the ceiling plus a random share of the other half ("equal
  jitter"). That keeps a floor, so a burst of failures does not retry
  immediately, while still spreading retries out.
- After `max_attempts` the item becomes `dead`. A permanent model error (an HTTP
  4xx from the backend, a rejected prompt) skips the remaining attempts.
- Time comes from the database (`now()`), not from worker clocks, so skew
  between workers cannot make leases expire early or late.

## Failure modes

| Failure | What happens | Where it is handled |
|---|---|---|
| Worker process crashes mid-item | It stops heartbeating. The lease expires after `LEASE_DURATION`. Another worker reclaims the item (attempt count goes up). | `Queue.Lease`, test `TestCrashedWorkerItemIsRetriedAfterLeaseExpiry` |
| Worker crashes on the final attempt | `Lease` does not reclaim items with no attempts left. The reaper marks them dead with `lease expired after final attempt`. | `Queue.ReapExpired` |
| Slow model call outlives the lease | A heartbeat extends leases of in-flight items every third of the lease. | `Pool.heartbeat` |
| Worker is paused (GC, network partition) and loses its lease, then wakes | Its `Complete` is rejected by the fence. The metric `iq_leases_lost_total` increments. The new owner's result stands. | fenced updates |
| Graceful shutdown (SIGTERM, deploy) | Stop leasing, wait up to the drain timeout for in-flight items, abort the rest, release their leases and refund the attempt. | `Pool.Run` |
| Worker killed by SIGKILL after the grace period | Same as a crash; recovery after lease expiry. | lease expiry |
| Model returns 429 or 5xx | The client retries inside the call, honouring `Retry-After` (capped). If it still fails the item is retried later with backoff. | `model.OpenAI`, `Queue.Fail` |
| Model backend is down | After N consecutive failures the breaker opens and calls fail fast. Workers requeue items without charging attempts, so an outage does not dead-letter healthy work (the few calls that trip the breaker are charged). | `model.Breaker`, `Queue.Requeue` |
| Model rejects a prompt (4xx) | Permanent error, dead-lettered immediately, other items unaffected. | `model.ErrPermanent` |
| Redis is down | Cache lookups count as misses; the rate limiter falls back to a per-process bucket. Both increment `iq_dependency_errors_total`. | `FailOpen`, cache error handling |
| Postgres is briefly unavailable | Lease and completion calls fail and are logged. Items stay leased and are recovered by lease expiry if a completion write was lost. `/readyz` fails so the load balancer stops sending traffic. | pool errors, `Ready` |
| Duplicate batch submission (client retry) | Same `Idempotency-Key` and body returns the original batch. A different body returns 409. Concurrent identical requests produce one batch. | `Batches.Create` |
| Webhook endpoint is down | Retries with backoff, then dead after `MaxAttempts`. State is visible in the `outbox` table. | `events.Publisher` |
| Publisher crashes after sending, before marking delivered | The row is claimed again after `locked_until` and the event is sent twice. Receivers de-duplicate on `X-IQ-Event-Id`. | at-least-once |
| Batch row contention | Every completion updates its batch row, which serialises completions within one batch. Throughput is still hundreds per second per batch in BENCHMARKS.md. | known limit, below |
| Oversized or malformed requests | Body size cap, item count cap, prompt size cap, unknown JSON fields rejected. | `server.validate` |

## Delivery semantics: at-least-once versus exactly-once

- **Model calls are at-least-once.** If a worker finishes a model call and dies
  before its `Complete` commits, the item is retried and the model is called
  again. A distributed system cannot avoid this without the model backend taking
  part in a transaction. The cache narrows the window: if the first call reached
  the cache, the retry is a cache hit and costs nothing.
- **Result recording is effectively exactly-once.** Fencing ensures at most one
  worker's result is stored for a given attempt, and the batch counters are
  updated in the same transaction as the item, so a batch cannot count an item
  twice. The concurrency test asserts that with 16 concurrent workers over 400
  items every item ends `succeeded` with `attempts = 1` and the model was called
  exactly 400 times.
- **Batch creation is exactly-once from the client's view** when the client sends
  an `Idempotency-Key`.
- **Webhooks are at-least-once.** Each has a stable id and a signature.
- **Ordering.** Items are claimed roughly in submission order but may complete in
  any order. Results are returned ordered by item index.

If the model backend is non-deterministic, a retried item may produce a
different output than the lost first attempt. Only the recorded one is visible.

## Caching and stampedes

The cache key is `sha256(len:model | len:params | len:prompt)` with length
prefixes so field boundaries cannot collide. Params are canonicalised to sorted
JSON at submission, so `{"a":1,"b":2}` and `{"b":2,"a":1}` share entries.

Two layers protect the model from duplicate work:

1. The shared cache (Redis, or in-process memory).
2. A `singleflight` group in each worker process: concurrent items with the same
   key share one model call, and the result is written to the cache before the
   flight ends.

Across processes there is still a window where two workers miss at the same
moment and both call the model. That is accepted: the cost is one extra call per
key per race, and a distributed lock would add its own failure modes. If it
mattered, a short `SET NX` lease per key would close it.

Caching is only appropriate when the model output is acceptable to reuse, which
is true for temperature-zero evals and false for sampling. A batch can opt out
by running with a worker configured with `CACHE_DISABLED=true`; per-batch opt-out
is not implemented.

## Rate limiting

A token bucket per API key, applied to every authenticated request. The Redis
version is a single Lua script (atomic read, refill, take, write) that uses Redis
`TIME`, so API replicas with skewed clocks share one view. Bucket keys expire
after they would have refilled, so idle keys do not accumulate. If Redis errors,
the request is checked against an in-process bucket instead, which makes the
limit per replica rather than global but keeps the API up.

The limiter protects the API. It does not throttle model calls; worker
concurrency and the circuit breaker do that.

## Authentication and authorization

Keys are `iq_` plus 48 hex characters from `crypto/rand`. Only `sha256(key)` is
stored. A fast hash is appropriate here because keys are long and random, so
there is nothing to brute-force; a password KDF would only add latency to every
request. The stored hash is compared with `subtle.ConstantTimeCompare` after the
lookup. Scopes are `submit`, `read` and `admin`, and `admin` implies the others.
A key sees only the batches it created unless it is `admin`; other keys get 404
so ids cannot be probed. Revoked keys stop working on the next request.

The bootstrap admin key from the environment is the root of trust. Treat it like
any other secret and rotate by creating a new admin key and revoking the old one.

## Event stream and outbox

SSE polls the batch row at a short interval and emits an event when the counters
change. This was chosen over LISTEN/NOTIFY because it is correct across API
replicas and connection poolers in transaction mode, and one primary-key read per
open stream per interval is cheap. Streams end with a `done` event, or a
`shutdown` event if the server is stopping.

`batch.completed` is written to `outbox` inside the transaction that completes
the batch. The publisher claims due rows with `SKIP LOCKED`, sets a short
`locked_until`, posts the payload with an HMAC signature
(`t=<unix>,v1=<hex>` over `<unix>.<body>`), and records the outcome. The HTTP
client refuses loopback, private and link-local destinations at dial time, which
also covers DNS rebinding and redirects.

## Observability

- Logs: `slog` JSON with a request id (accepted from `X-Request-Id` or generated),
  route pattern, status and duration.
- Metrics: queue depth by state, lease query duration, queue wait, model latency
  histogram by outcome, items by outcome, cache hits and misses (hit ratio is
  `hit / (hit + miss)`), HTTP rate and latency by route, rate-limited requests,
  lost leases, breaker state, webhook attempts.
- Tracing: OpenTelemetry spans for requests and for each item. The global tracer
  is a no-op until `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
- Probes: `/healthz` is liveness only; `/readyz` checks Postgres. Redis is not part
  of readiness because the service is designed to run without it.

## Known limits and what I would do next

- **Counter contention.** One batch row takes an update per completed item.
  Completions within a very large batch serialise on that row. The fix is to
  accumulate counters in a side table keyed by batch and shard and sum on read.
- **No retention.** Finished batches and items are kept. A scheduled job should
  delete or partition by `created_at`.
- **No cancellation or priorities.** Both are straightforward: a `cancelled`
  status checked in the lease query, and a `priority` column in the ordering and
  index.
- **Polling SSE and publisher.** Fine at this scale. LISTEN/NOTIFY as a wake-up
  hint (with polling as the safety net) would cut latency and idle load.
- **Cross-process cache stampede window** as described above.
- **Per-batch cache opt-out** and per-model concurrency limits.
- **Fairness.** Items are claimed in `run_after, id` order, so one very large batch
  can delay a small one submitted later. A per-key round-robin or a priority
  would address it.
