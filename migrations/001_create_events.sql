CREATE TYPE delivery_status AS ENUM ('pending', 'processing', 'delivered', 'failed');

CREATE TABLE events (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type       TEXT NOT NULL,
    payload          JSONB NOT NULL,
    status           delivery_status NOT NULL DEFAULT 'pending',
    idempotency_key  TEXT NOT NULL UNIQUE,
    retry_count      INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at        TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX events_due_idx ON events (next_attempt_at) WHERE status = 'pending';