CREATE TYPE delivery_status AS ENUM ('pending', 'processing', 'delivered', 'failed');

CREATE TABLE deliveries (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id         UUID NOT NULL REFERENCES events(id),
    subscription_id  UUID NOT NULL REFERENCES subscriptions(id),
    status           delivery_status NOT NULL DEFAULT 'pending',
    retry_count      INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at        TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX deliveries_due_idx ON deliveries (next_attempt_at) WHERE status = 'pending';