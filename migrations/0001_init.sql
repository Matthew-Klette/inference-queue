CREATE TABLE api_keys (
    id          uuid PRIMARY KEY,
    name        text        NOT NULL,
    key_hash    bytea       NOT NULL UNIQUE,
    scopes      text[]      NOT NULL,
    rate_rps    double precision NOT NULL DEFAULT 20,
    rate_burst  integer     NOT NULL DEFAULT 40,
    created_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at  timestamptz
);

CREATE TABLE batches (
    id              uuid PRIMARY KEY,
    api_key_id      uuid        NOT NULL REFERENCES api_keys (id),
    model           text        NOT NULL,
    params          jsonb       NOT NULL DEFAULT '{}',
    status          text        NOT NULL DEFAULT 'queued'
                    CHECK (status IN ('queued', 'running', 'completed', 'completed_with_errors')),
    total           integer     NOT NULL,
    succeeded       integer     NOT NULL DEFAULT 0,
    failed          integer     NOT NULL DEFAULT 0,
    scored          integer     NOT NULL DEFAULT 0,
    matched         integer     NOT NULL DEFAULT 0,
    webhook_url     text,
    idempotency_key text,
    request_hash    bytea       NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    completed_at    timestamptz
);

-- One idempotency key per API key. Partial so that NULL keys never collide.
CREATE UNIQUE INDEX batches_idempotency_idx
    ON batches (api_key_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX batches_owner_created_idx ON batches (api_key_id, created_at DESC);

CREATE TABLE items (
    id                bigserial PRIMARY KEY,
    batch_id          uuid        NOT NULL REFERENCES batches (id) ON DELETE CASCADE,
    idx               integer     NOT NULL,
    ref               text,
    prompt            text        NOT NULL,
    expected          text,
    status            text        NOT NULL DEFAULT 'queued'
                      CHECK (status IN ('queued', 'leased', 'succeeded', 'dead')),
    attempts          integer     NOT NULL DEFAULT 0,
    max_attempts      integer     NOT NULL DEFAULT 5,
    run_after         timestamptz NOT NULL DEFAULT now(),
    lease_owner       text,
    lease_expires_at  timestamptz,
    result            text,
    matched           boolean,
    cached            boolean     NOT NULL DEFAULT false,
    prompt_tokens     integer,
    completion_tokens integer,
    latency_ms        integer,
    last_error        text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    completed_at      timestamptz,
    UNIQUE (batch_id, idx)
);

-- The hot path for leasing: only rows that can still be picked up are indexed.
CREATE INDEX items_ready_idx ON items (run_after, id) WHERE status = 'queued';
CREATE INDEX items_lease_idx ON items (lease_expires_at) WHERE status = 'leased';
CREATE INDEX items_dead_idx  ON items (batch_id) WHERE status = 'dead';

CREATE TABLE outbox (
    id              bigserial PRIMARY KEY,
    batch_id        uuid        NOT NULL REFERENCES batches (id) ON DELETE CASCADE,
    event_type      text        NOT NULL,
    payload         jsonb       NOT NULL,
    webhook_url     text        NOT NULL,
    attempts        integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    locked_until    timestamptz,
    delivered_at    timestamptz,
    dead            boolean     NOT NULL DEFAULT false,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX outbox_due_idx ON outbox (next_attempt_at) WHERE delivered_at IS NULL AND NOT dead;
