import { pool } from "./db";

type EventInput = {
  eventType: string;
  payload: unknown;
  key: string;
};

type InsertResult =
  | { created: true; id: string; deliveries: number }
  | { created: false };

export const insertEvent = async (
  eventData: EventInput,
): Promise<InsertResult> => {
  const { eventType, payload, key } = eventData;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // 1. Insert the event. ON CONFLICT DO NOTHING dedupes on idempotency_key.
    //    RETURNING id tells us if a row was actually inserted (new) or skipped (dupe).
    const eventResult = await client.query(
      `INSERT INTO events (event_type, payload, idempotency_key)
       VALUES ($1, $2, $3)
       ON CONFLICT (idempotency_key) DO NOTHING
       RETURNING id`,
      [eventType, payload, key],
    );

    // Duplicate event: nothing inserted, so no fan-out. Commit (no-op) and bail.
    if (eventResult.rows.length === 0) {
      await client.query("COMMIT");
      return { created: false };
    }

    const eventId = eventResult.rows[0].id;

    // 2. Insert one 'pending' delivery per matching active subscription,
    //    in a single INSERT ... SELECT. This reads the subscriptions and
    //    writes the delivery rows in one statement — no loop needed.
    //    $1 = eventId (same for every row), sub.id comes from the SELECT.
    const deliveryResult = await client.query(
      `INSERT INTO deliveries (event_id, subscription_id)
       SELECT $1, s.id
       FROM subscriptions s
       WHERE s.event_type = $2 AND s.is_active = true`,
      [eventId, eventType],
    );

    // 3. Both writes succeeded together — commit atomically.
    await client.query("COMMIT");

    return {
      created: true,
      id: eventId,
      deliveries: deliveryResult.rowCount ?? 0,
    };
  } catch (error) {
    await client.query("ROLLBACK"); // any failure → undo BOTH inserts
    throw error;
  } finally {
    client.release();
  }
};
