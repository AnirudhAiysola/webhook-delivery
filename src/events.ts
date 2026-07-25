import { pool } from "./db";

type EventInput = {
  eventType: string;
  payload: unknown;
  key: string;
};

type InsertResult = { created: true; id: string } | { created: false };

export const insertEvent = async (
  eventData: EventInput,
): Promise<InsertResult> => {
  const { eventType, payload, key } = eventData;

  const result = await pool.query(
    `INSERT INTO events (event_type, payload, idempotency_key)
     VALUES ($1, $2, $3)
     ON CONFLICT (idempotency_key) DO NOTHING
     RETURNING id`,
    [eventType, payload, key],
  );

  if (result.rows.length > 0) {
    return { created: true, id: result.rows[0].id };
  }

  return { created: false };
};

// docker compose exec postgres psql -U webhook -d webhook
