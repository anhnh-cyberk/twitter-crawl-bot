import pg from "pg";
import * as dotenv from "dotenv";
dotenv.config();
export function getPostgreConnection() {
  console.log(`Connecting to PostgreSQL database: ${process.env.DB_NAME} on ${process.env.DB_HOST}:${process.env.DB_PORT}`);
  const pool = new pg.Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: parseInt(process.env.DB_PORT || "5432"),
    ssl: {
      rejectUnauthorized: false,
    },
  });
  return pool;
}
