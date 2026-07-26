import { pool } from "./db";

type SubscriptionInput = {
  eventType: string;
  url: string;
};

type InsertResult = { created: true; id: string } | { created: false };

export const isValidWebhookUrl = (value: unknown): value is string => {
  if (typeof value !== "string") return false;

  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    return false; // not a parseable URL at all
  }

  // Only allow http/https — we're going to POST to this, so
  // reject things like ftp:, file:, javascript:, etc.
  return parsed.protocol === "http:" || parsed.protocol === "https:";
};

export const insertSubscription = async (
  data: SubscriptionInput,
): Promise<{ id: string }> => {
  const { eventType, url } = data;
  const result = await pool.query(
    `INSERT INTO subscriptions (event_type, url) VALUES ($1, $2) RETURNING id`,
    [eventType, url],
  );
  return { id: result.rows[0].id };
};
