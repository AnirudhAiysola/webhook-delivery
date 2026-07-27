# Webhook Delivery Service

The reliable sending side of a webhook system, like what Stripe or GitHub run when they call your endpoint. Accepts events, fans them out to subscribers, and guarantees delivery even when subscribers are down, slow, or flaky.

**The hard part isn't sending a POST. It's what happens when the receiver is broken.** This project is built entirely around that.

<img width="1840" height="1280" alt="image" src="https://github.com/user-attachments/assets/b972abe8-8ee5-4e71-8edb-ea39237e8a00" />


## The interesting decisions

Most of this project is about failure handling. Here are the calls I had to reason through, and why.

**Postgres is the queue — no SQS, no Kafka, no Redis.**
At this scale a dedicated broker is complexity without benefit. Keeping everything in one database means an event and its deliveries commit in a single transaction, so there's no dual-write problem to solve because there aren't two systems. If throughput outgrew one Postgres instance, I'd move to SQS and add the outbox pattern. Not before.

**At-least-once delivery, not exactly-once.**
Exactly-once is a myth in distributed systems: a delivery can succeed and still look failed if the ack is lost. So every event carries an idempotency key with a unique constraint, and a duplicate insert is rejected by the database, not by application code. A `check-then-insert` has a race; `INSERT ... ON CONFLICT DO NOTHING` is atomic. The database is the check.

**`SELECT ... FOR UPDATE SKIP LOCKED` for concurrent workers.**
`FOR UPDATE` locks a claimed row so no two workers take the same job. `SKIP LOCKED` makes other workers skip locked rows instead of blocking on them, so N workers pull disjoint batches in parallel. The lock is held only for the fast claim; a `processing` status guards the slow HTTP delivery after the lock is released.

**Exponential backoff + jitter, then dead-letter.**
Backoff so a struggling subscriber isn't hammered. Jitter so a batch of failures doesn't retry in one synchronized wave and re-crush a recovering server. After a retry cap, the delivery is marked `failed` — a logical dead-letter queue kept for inspection and replay.

**Crash recovery via reclaim.**
A worker that dies mid-delivery would strand its job in `processing` forever. A reclaim step resets deliveries stuck too long back to `pending`. It can re-send a duplicate, which is fine, because delivery is idempotent anyway.

**Per-subscriber delivery state.**
Delivery state lives on a `deliveries` table, one row per event-subscriber pair. If an event fans out to three subscribers and one fails, only that one retries. The other two are never contacted again.

## How it works

Two processes that never talk directly — they coordinate only through the database.

- **API** accepts events, dedupes them, and does a transactional fan-out into one `pending` delivery per matching subscriber. Returns `202` immediately and does no delivery work itself.
- **Worker** runs a poll loop: reclaim stranded deliveries, claim a due batch with `SKIP LOCKED`, POST to each subscriber, record the outcome, sleep, repeat.

This is a **pull** model. An event doesn't trigger anything, it becomes a row the worker finds on its next poll. That's why it needs always-on compute (EC2), not Lambda. `LISTEN/NOTIFY` would make it push-based and is a natural next step.

## Stack

TypeScript · Node · Express · raw `pg` (no ORM, on purpose — this project is about the things an ORM hides) · Postgres · Docker · AWS EC2 + RDS

## Run it locally

```bash
docker compose up -d                 # start Postgres
# apply migrations in order
for f in migrations/*.sql; do
  docker compose exec -T postgres psql -U webhook -d webhook < "$f"
done
npm run dev                          # API
npm run dev:worker                   # worker (separate terminal)
```

```bash
# register a subscriber (use a webhook.site bin)
curl -X POST localhost:3000/subscription \
  -H 'Content-Type: application/json' \
  -d '{"eventType":"order.created","url":"https://webhook.site/your-bin"}'

# send an event and watch it arrive
curl -X POST localhost:3000/events \
  -H 'Content-Type: application/json' \
  -d '{"eventType":"order.created","payload":{"orderId":1},"key":"k1"}'
```

## Roadmap

- Load test showing throughput scaling with worker count
- CI/CD via GitHub Actions
- Read-only dashboard (events, delivery status, retry counts, replay)
- HMAC payload signing · SSRF protection on subscriber URLs · event-type registry
