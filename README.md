# Webhook Delivery Service

A service that reliably delivers webhook events to subscriber endpoints, built to survive the messy reality of the internet. Subscribers go down, servers time out, responses get lost, and this service keeps trying until the event gets through or it gives up in a controlled way. It is the sending side of the kind of system Stripe or GitHub run when they call your endpoint after something happens.

The interesting part of this project is not that it moves data from A to B. It is what happens when B is broken.

![Architecture](architecture.svg)

## What it does

An event arrives over HTTP. The service stores it, works out which subscribers care about that kind of event, and creates one delivery per subscriber. A background worker picks up due deliveries and posts each event to its subscriber URL. If a delivery fails, it backs off and tries again later, waiting longer after each failure. After enough failures it stops retrying and parks the delivery in a dead state for inspection. Nothing is lost along the way, even if a worker crashes mid delivery.

## Design decisions

I wanted to understand each choice rather than reach for the default, so here is the reasoning behind the main ones.

### Postgres is the queue

There is no SQS, no Kafka, no Redis. Postgres itself is the queue. Deliveries live in a table and the worker pulls due rows from it.

The usual instinct is to add a dedicated message broker, but short of very high throughput that is complexity without benefit. A single Postgres instance handles thousands of jobs per second, more than most systems ever need. A separate broker is another thing to run, secure, monitor, and pay for.

More importantly, keeping everything in one database dissolves the dual write problem. If the queue were a separate system, ingesting an event would mean writing to the database and writing to the queue, two systems with no shared transaction, and a crash between the two writes loses data. With Postgres as the queue, the event and its delivery rows are inserted in a single transaction. They both commit or both roll back, so there is never an event with no deliveries.

When I would change it: if a single Postgres instance could not keep up, or if many independent services needed to consume the same events, I would move to SQS or Kafka and add the outbox pattern to bridge the database write to the external queue safely.

### At least once delivery, not exactly once

Exactly once delivery does not really exist in a distributed system. A delivery can succeed on the subscriber side and still look like a failure if the acknowledgement is lost. So instead of pretending, the service guarantees at least once delivery and makes duplicates harmless.

Every event carries an idempotency key with a unique constraint in the database. A duplicate ingest is rejected by the database itself, not by a check in application code. This matters because a check then insert has a race condition, two identical requests can both pass the check before either inserts. A single insert with `ON CONFLICT DO NOTHING` is atomic, so the database is the check.

The same idea shows up in three places, retries, crash recovery, and repeated ingest. They all can produce a duplicate, and they all lean on the same fix, make the operation safe to repeat and let idempotency absorb the duplicate.

### SKIP LOCKED for safe concurrent workers

The worker claims due deliveries with `SELECT ... FOR UPDATE SKIP LOCKED` inside a transaction.

`FOR UPDATE` locks the selected rows so no two workers grab the same delivery. On its own that would make a second worker block and wait on rows the first already locked, which wastes the point of running more than one worker. `SKIP LOCKED` means a worker skips locked rows and moves to the next available ones, so any number of workers run in parallel pulling disjoint batches.

The lock is only held for the brief claim. The worker marks the rows as processing, commits, and releases the lock before making any slow HTTP calls. The lock protects the fast claim, the processing status protects the slow delivery. Two mechanisms for two different windows.

Because the workers are stateless and coordinate only through the database, scaling is just running more of them, more containers or more machines, with no code change.

### Retry with exponential backoff and jitter

When a delivery fails the service does not retry immediately. Hammering a subscriber that is already struggling makes its outage worse and burns worker cycles on a dead endpoint. So each failure pushes the next attempt further out, doubling the wait each time.

On top of the backoff there is jitter, a random spread added to each delay. Without it, a batch of deliveries that all failed at the same moment would all retry at the same moment, slamming the recovering subscriber in synchronized waves. Jitter scatters the retries across a window so the load arrives as a trickle. This follows the approach in AWS's Builders' Library piece on timeouts, retries, and backoff with jitter, which was a useful reference for the details.

After a fixed number of failed attempts the delivery is marked failed. That failed state is a logical dead letter queue. The row stays in the table for inspection and replay rather than being deleted, and the worker ignores it because it only claims pending rows.

### Surviving a worker crash

A worker marks a delivery as processing while it works on it, hiding it from other workers. If that worker dies mid delivery, the row would be stuck as processing forever, claimed by nobody and finished by nobody.

To handle this, each claim is stamped with a timestamp. A reclaim step finds any delivery that has been processing longer than a delivery could reasonably take, assumes its worker has died, and resets it to pending so a healthy worker picks it up again. This is the safety net that keeps a crash from silently stranding an event. It can cause a duplicate if the worker actually finished just before dying, which is fine, because duplicates are already safe.

### Per subscriber delivery state

Delivery state lives on a deliveries table, one row per event and subscriber pair, not on the event itself. An event can fan out to many subscribers, and each delivery succeeds, fails, and retries independently. If two subscribers succeed and a third fails, only the third is retried. A single status on the event could not express that, it would force an all or nothing retry that re-sends to subscribers who already received the event.

## How it fits together

The system runs as two processes that never talk to each other directly. They coordinate only through the database.

The API process handles HTTP. It accepts events, registers subscriptions, and does the transactional fan out that turns one event into one pending delivery per matching subscriber. It responds immediately with a 202 and does no delivery work itself, so a slow or failing subscriber never makes the caller wait.

The worker process runs a continuous loop. It reclaims stranded deliveries, claims a batch of due deliveries, posts each to its subscriber, and records the outcome, then sleeps briefly and repeats. It has no HTTP server. It pulls work from the database rather than being triggered by it, which is why it needs to be always running.

This is a pull model, not a push model. An arriving event does not wake anything up, it becomes a row that waits until the worker finds it on the next poll. The tradeoff is a small amount of latency in exchange for a simpler system. A push model using Postgres `LISTEN` and `NOTIFY` would cut that latency and is a natural later improvement, though a periodic poll would still be needed to catch retries that become due by the clock.

## Data model

Three tables.

`events` records what happened, the event type, the payload, and the idempotency key that makes ingest safe to retry.

`subscriptions` records who wants what, an event type and a destination URL, plus an active flag so a subscription can be paused without being deleted.

`deliveries` is the queue and the record of work. One row per event and subscriber pair, carrying its own status, retry count, next attempt time, and lock timestamp. It references events and subscriptions with foreign keys, because a delivery belongs to both.

## Tech stack

- TypeScript and Node, Express for the API
- `pg` with raw SQL and no ORM, chosen deliberately because this project is about the exact things an ORM hides, row locking, transaction control, and `ON CONFLICT`
- Postgres for storage and as the queue
- Docker for packaging, deployed on AWS with EC2 running the containers and RDS for managed Postgres

## Running it locally

You need Docker.

Start Postgres:

```
docker compose up -d
```

Apply the migrations in order:

```
docker compose exec -T postgres psql -U webhook -d webhook < migrations/001_create_events.sql
docker compose exec -T postgres psql -U webhook -d webhook < migrations/002_create_subscriptions.sql
docker compose exec -T postgres psql -U webhook -d webhook < migrations/003_create_deliveries.sql
```

Run the API and the worker in separate terminals:

```
npm run dev
npm run dev:worker
```

Register a subscription pointing at a URL you control, such as a [webhook.site](https://webhook.site) bin:

```
curl -X POST http://localhost:3000/subscription \
  -H "Content-Type: application/json" \
  -d '{"eventType":"order.created","url":"https://webhook.site/your-bin-id"}'
```

Send an event of the matching type and watch it arrive at the subscriber:

```
curl -X POST http://localhost:3000/events \
  -H "Content-Type: application/json" \
  -d '{"eventType":"order.created","payload":{"orderId":1},"key":"unique-key-1"}'
```

Configuration comes from environment variables, mainly `DATABASE_URL`, so the same build runs locally and in production with only that value changing.

## API

`POST /events` — ingest an event. Body: `eventType`, `payload`, `key` (idempotency key). Returns 202 for a new event, 200 if the key was already seen.

`POST /subscription` — register a subscriber. Body: `eventType`, `url`. Returns 201.

`GET /health` — liveness check. Returns 200.

## What is not built yet

A few things are deliberately left as next steps rather than half done.

- CI/CD. Deploys are currently manual, pull the latest code on the server and rebuild. Automating that with GitHub Actions is the next piece of work.
- A dashboard. A read mostly frontend showing events, delivery status, retry counts, and a replay button.
- Payload signing. Signing each delivery with an HMAC so subscribers can verify it came from us and was not tampered with.
- SSRF protection. The service posts to user supplied URLs, so production needs to block private and link local IP ranges and validate the resolved IP, not just the hostname.
- A registry of valid event types, so a typo in an event type is caught at insert rather than causing silent non delivery.
- Retention. Delivered rows are kept for history and would be aged out by a scheduled sweep rather than growing forever.
