import { Pool } from "pg";

const useSSL = process.env.DATABASE_URL?.includes("rds.amazonaws.com");

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: useSSL ? { rejectUnauthorized: false } : false,
});

pool.on("error", (err) => {
  console.error("Unexpected error on idle Postgres client", err);
  process.exit(1);
});
