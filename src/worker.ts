import "dotenv/config";
import { pool } from "./db";

// A claimed delivery, joined with the data needed to actually send it.
type ClaimedDelivery = {
  delivery_id: string;
  event_id: string;
  retry_count: number;
  payload: unknown; // from events
  url: string; // from subscriptions
};

// STEP 1: claim a batch of due deliveries.
// JOIN pulls in the event payload and subscription url in one shot.
// FOR UPDATE OF deliveries → lock ONLY the delivery rows, not the
// joined events/subscriptions rows (we're not claiming those).
async function claimDeliveries(): Promise<ClaimedDelivery[]> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const result = await client.query(
      `SELECT d.id AS delivery_id, d.event_id, d.retry_count,
              e.payload, s.url
       FROM deliveries d
       JOIN events e ON e.id = d.event_id
       JOIN subscriptions s ON s.id = d.subscription_id
       WHERE d.status = 'pending' AND d.next_attempt_at <= now()
       ORDER BY d.next_attempt_at ASC
       LIMIT 10
       FOR UPDATE OF d SKIP LOCKED`,
    );

    const ids = result.rows.map((r) => r.delivery_id);
    if (ids.length === 0) {
      await client.query("COMMIT");
      return [];
    }

    // Mark claimed deliveries as processing so no other worker touches them
    // after we release the lock (the slow HTTP call happens outside the tx).
    await client.query(
      `UPDATE deliveries
       SET status = 'processing', locked_at = now()
       WHERE id = ANY($1)`,
      [ids],
    );

    await client.query("COMMIT");
    return result.rows as ClaimedDelivery[];
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

const MAX_RETRIES = 5; // give up after this many failed attempts → dead-letter

// Compute the delay before the next attempt, in seconds.
// Exponential backoff: 2^retryCount → 1, 2, 4, 8, 16 ...
// Plus jitter: randomize so many failed deliveries for the same downed
// subscriber don't all retry at the exact same instant (thundering herd).
function backoffSeconds(retryCount: number): number {
  const base = Math.pow(2, retryCount); // exponential
  const jitter = Math.random() * base; // 0 .. base random spread
  return base + jitter;
}

// Called whenever a delivery attempt fails (non-2xx OR network error).
// Decides: retry later, or give up and dead-letter.
async function handleFailure(delivery: ClaimedDelivery): Promise<void> {
  const nextRetry = delivery.retry_count + 1;

  if (nextRetry >= MAX_RETRIES) {
    // Retries exhausted → dead-letter it. Status 'failed' = our logical DLQ.
    await pool.query(
      `UPDATE deliveries
       SET status = 'failed', retry_count = $2, locked_at = NULL
       WHERE id = $1`,
      [delivery.delivery_id, nextRetry],
    );
    console.log(
      `dead-lettered ${delivery.delivery_id} after ${nextRetry} attempts`,
    );
    return;
  }

  // Still have retries left → schedule the next attempt with backoff.
  // Back to 'pending' so the poller picks it up again once it's due.
  // locked_at = NULL because it's no longer being processed.
  const delay = backoffSeconds(nextRetry);
  await pool.query(
    `UPDATE deliveries
     SET status = 'pending',
         retry_count = $2,
         next_attempt_at = now() + ($3 || ' seconds')::interval,
         locked_at = NULL
     WHERE id = $1`,
    [delivery.delivery_id, nextRetry, delay.toString()],
  );
  console.log(
    `retry ${nextRetry} for ${delivery.delivery_id} in ~${delay.toFixed(1)}s`,
  );
}

// STEP 2 + 3: deliver one claimed delivery, now with failure handling.
async function deliverOne(delivery: ClaimedDelivery): Promise<void> {
  try {
    // Timeout so a hanging subscriber can't block the worker forever.
    // (The 'timeouts' pillar from the AWS article.)
    const response = await fetch(delivery.url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(delivery.payload),
      signal: AbortSignal.timeout(10000), // 10s
    });

    if (response.ok) {
      // 2xx → success. Terminal state; freeze the row as-is.
      await pool.query(
        `UPDATE deliveries SET status = 'delivered', locked_at = NULL WHERE id = $1`,
        [delivery.delivery_id],
      );
      console.log(`delivered ${delivery.delivery_id} → ${delivery.url}`);
    } else {
      // Reached them, but non-2xx → failure. Retry or dead-letter.
      console.log(`failed ${delivery.delivery_id}: HTTP ${response.status}`);
      await handleFailure(delivery);
    }
  } catch (error) {
    // Network-level failure OR timeout → couldn't get a valid response.
    // Same treatment: retry or dead-letter.
    console.log(`error delivering ${delivery.delivery_id}:`, error);
    await handleFailure(delivery);
  }
}
const POLL_INTERVAL_MS = 1000; // how long to sleep between polls
const RECLAIM_AFTER_SECONDS = 30; // a 'processing' row older than this = dead worker

// RECLAIM: rescue deliveries orphaned by a crashed worker.
// A row stuck in 'processing' with an old locked_at means the worker that
// claimed it died before finishing. Reset it to 'pending' so a healthy
// worker picks it up again. Without this, a crash strands the delivery forever.
async function reclaimStuck(): Promise<void> {
  const result = await pool.query(
    `UPDATE deliveries
     SET status = 'pending', locked_at = NULL
     WHERE status = 'processing'
       AND locked_at < now() - ($1 || ' seconds')::interval`,
    [RECLAIM_AFTER_SECONDS.toString()],
  );
  if (result.rowCount && result.rowCount > 0) {
    console.log(`reclaimed ${result.rowCount} stuck deliveries`);
  }
}

// Small sleep helper for the loop.
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// THE WORKER: run forever. Each cycle: reclaim orphans, claim due
// deliveries, deliver each, sleep, repeat.
async function worker(): Promise<void> {
  console.log("worker started");
  while (true) {
    try {
      await reclaimStuck(); // rescue crashed-worker orphans first

      const deliveries = await claimDeliveries();
      for (const d of deliveries) {
        await deliverOne(d); // each failure isolated by its own try/catch
      }
    } catch (error) {
      // A cycle failed (e.g. DB blip). Log and keep looping — don't crash the worker.
      console.error("worker cycle error:", error);
    }

    await sleep(POLL_INTERVAL_MS); // short-poll: wait, then poll again
  }
}

worker();
