# Runbook

Commands assume `psql "$DATABASE_URL"` for SQL and `$ADMIN` for an admin API
key. Metric names are those exported on `/metrics` by both the API and the
worker.

## First look

```sql
-- Queue state at a glance
SELECT status, count(*) FROM items GROUP BY status;

-- Same numbers as the iq_queue_items gauge
SELECT
  count(*) FILTER (WHERE status = 'queued' AND run_after <= now()) AS ready,
  count(*) FILTER (WHERE status = 'queued' AND run_after >  now()) AS delayed,
  count(*) FILTER (WHERE status = 'leased')                        AS leased
FROM items WHERE status IN ('queued', 'leased');
```

Useful signals: `iq_queue_items{state="ready"}` growing means workers are
behind; `iq_items_processed_total{outcome="retried"}` rising means the model is
failing; `iq_leases_lost_total` rising means leases are too short or workers are
stalling; `iq_model_breaker_open == 1` means the circuit breaker is open.

## Stuck leases

Symptoms: items in `leased` with an old `lease_expires_at`, a batch that stops
progressing, `leased` count that does not fall.

A lease on a live worker is renewed every third of `LEASE_DURATION`. An expired
lease is normal after a crash and is reclaimed automatically by the next
`Lease` call, so first check that a worker is running at all.

```sql
-- Expired leases that nobody has reclaimed
SELECT id, batch_id, lease_owner, attempts, max_attempts,
       now() - lease_expires_at AS overdue
FROM items
WHERE status = 'leased' AND lease_expires_at < now()
ORDER BY lease_expires_at LIMIT 50;
```

- Overdue items with `attempts < max_attempts` are reclaimed by any running
  worker within one poll interval. If they are not, no worker is leasing:
  check worker pods, `/readyz`, and the worker logs for `lease failed`.
- Overdue items with `attempts >= max_attempts` are dead-lettered by the reaper
  (runs every few seconds in each worker) with
  `last_error = 'lease expired after final attempt'`.
- Which owners hold live leases:

```sql
SELECT lease_owner, count(*), min(lease_expires_at) AS soonest_expiry
FROM items WHERE status = 'leased' GROUP BY lease_owner;
```

If a lease owner is gone and you do not want to wait for expiry, make the
leases immediately reclaimable. The next lease counts as another attempt, the
same as normal expiry recovery:

```sql
UPDATE items SET lease_expires_at = now() - interval '1 second'
WHERE status = 'leased' AND lease_owner = 'host-1234';
```

If `iq_leases_lost_total` is increasing, workers are completing items after their
lease expired. Causes: `LEASE_DURATION` shorter than heartbeat can sustain
(database latency spikes), or a worker starved of CPU. Raise `LEASE_DURATION`.
The results are still correct, because stale completions are rejected, but the
model work was wasted.

## Dead-letter replay

Dead items are terminal: `status = 'dead'`, with `last_error` and the attempt
count. Find them:

```sql
SELECT batch_id, count(*), min(last_error) AS sample_error
FROM items WHERE status = 'dead' GROUP BY batch_id ORDER BY count(*) DESC;

SELECT idx, attempts, last_error FROM items
WHERE batch_id = '<batch id>' AND status = 'dead' ORDER BY idx;
```

Decide first whether replay can succeed. Permanent errors (the model rejected the
prompt) will fail again. Replay is appropriate after an outage, a bad deploy, or
a fixed model configuration.

```sh
curl -s -X POST localhost:8080/v1/batches/$BATCH/replay -H "Authorization: Bearer $ADMIN"
# {"replayed":12}
```

Replay sets those items to `queued` with a fresh attempt budget, decrements the
batch `failed` counter, and reopens the batch (`running`). When the batch
finishes again a new `batch.completed` event is emitted. To replay across many
batches after an incident:

```sql
SELECT DISTINCT batch_id FROM items
WHERE status = 'dead' AND completed_at > now() - interval '2 hours';
```

and call the endpoint for each id.

Dead webhook events are a separate table:

```sql
SELECT id, batch_id, attempts, last_error FROM outbox WHERE dead;
-- after fixing the receiver:
UPDATE outbox SET dead = false, attempts = 0, next_attempt_at = now() WHERE id = <id>;
```

## Cache stampede

Symptoms: a spike in `iq_model_duration_seconds_count` with a low hit ratio
right after a Redis flush or restart, a deploy that changed cache keys, or a
large new batch whose prompts repeat.

```
# hit ratio over 5 minutes
sum(rate(iq_cache_lookups_total{result="hit"}[5m]))
  / sum(rate(iq_cache_lookups_total[5m]))
```

What already protects you: identical concurrent prompts inside one worker share
a single model call, and the result is cached before the flight ends. What it
does not cover: two worker processes missing on the same key at the same moment
each call the model once.

If the model is being hit too hard after a cold cache:

1. Lower `WORKER_CONCURRENCY` or scale workers down for the warm-up. Items wait in
   the queue, which is the point of the queue.
2. Check Redis: `iq_dependency_errors_total{dependency="redis"}` increasing means
   the cache is being bypassed, not merely cold.
3. Confirm eviction is not the cause: `redis-cli info stats | grep evicted_keys`.
   The compose file uses `allkeys-lru` with 256 MB; size it for your working set.
4. If keys changed because params differ in whitespace or order, they are
   canonicalised on submit, so a real difference is in a parameter value.

Cache entries expire after `CACHE_TTL` (default 24 hours). To invalidate
everything deliberately, flush the Redis database; the service keeps running.

## Model backend outage

Symptoms: `iq_model_breaker_open == 1`, many `retried` outcomes, log lines
`item attempt failed` and `model circuit breaker changed state`, SSE streams that
stop moving.

What the system does by itself: the client retries 429 and 5xx with backoff and
honours `Retry-After`. After `BREAKER_FAILURES` consecutive failures the breaker
opens for `BREAKER_COOLDOWN`. While it is open, workers requeue items without
charging attempts and wait, so a long outage does not dead-letter healthy work.
After the cooldown one trial call is allowed; success closes the breaker.

What to do:

1. Check the backend directly (`curl $MODEL_BASE_URL/v1/models`) and its own
   health. If it is a 429 storm, reduce `WORKER_CONCURRENCY` rather than
   waiting for the breaker.
2. Do nothing to the queue. Items back up as `queued` and drain after recovery.
3. After recovery, look for items that were dead-lettered during the incident
   (see replay above). Items hit by the failures that tripped the breaker were
   charged an attempt, and with a low `max_attempts` a few may be dead.
4. If the model must be swapped, change `MODEL_BASE_URL` and restart workers. In
   flight items are released during shutdown and retried by the new workers.

If the model is up but slow, tune `MODEL_TIMEOUT` (per HTTP attempt) and make
sure `LEASE_DURATION` is not much shorter than a typical call. Heartbeats cover
long calls, but a very short lease plus a loaded database invites lost leases.

## Queue is growing and workers look idle

```sql
SELECT count(*) FILTER (WHERE run_after > now()) AS delayed_by_backoff,
       count(*) FILTER (WHERE run_after <= now()) AS ready
FROM items WHERE status = 'queued';
```

Many `delayed_by_backoff` means failures, not capacity. Many `ready` with low
worker CPU means model-bound work: add workers or raise `WORKER_CONCURRENCY`. The
HPA example in `k8s/hpa.yaml` scales on `iq_queue_items{state="ready"}`.

## Webhooks are not arriving

```sql
SELECT id, batch_id, attempts, next_attempt_at, dead, last_error
FROM outbox WHERE delivered_at IS NULL ORDER BY id LIMIT 20;
```

`iq_webhook_attempts_total` by outcome shows delivered, retry and dead. Check
that the receiver is reachable from the worker, that it is not on a private
address (set `WEBHOOK_ALLOW_PRIVATE=true` only in trusted networks), and that it
verifies `X-IQ-Signature` against the same `WEBHOOK_SECRET`. The publisher runs
inside the worker process and `api -with-worker`.

## A client is being rate limited

`429` with `Retry-After`. Limits are per key (`rate_rps`, `rate_burst`). There is
no endpoint to edit them in place; create a new key with higher limits and
revoke the old one. If Redis is down the limit becomes per replica, so a client
may see more throughput than configured, not less.

## Rotating or revoking a key

```sh
curl -s -X POST localhost:8080/v1/keys -H "Authorization: Bearer $ADMIN" \
  -H 'Content-Type: application/json' -d '{"name":"svc-b","scopes":["submit","read"]}'
curl -s -X DELETE localhost:8080/v1/keys/<old id> -H "Authorization: Bearer $ADMIN"   # 204
```

Revocation takes effect on the next request. Batches created by a revoked key
keep running and remain readable by admins.

## Deploys and shutdown

Workers handle SIGTERM by draining for up to `SHUTDOWN_TIMEOUT` minus five
seconds, then aborting what is left and releasing its leases. Set the pod
`terminationGracePeriodSeconds` above `SHUTDOWN_TIMEOUT` (the manifests use 40
against 30). After a SIGKILL, work is recovered by lease expiry instead, so the
cost is a delay of up to `LEASE_DURATION`, not lost work. Migrations run at
startup under an advisory lock; they are additive, so old and new versions can
overlap during a rolling update.
