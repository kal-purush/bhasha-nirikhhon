import pg, { type Client } from "pg";
import type { EnvDbConfig } from "../../plugins/external/env.js";
import { POSTGRES_DB_NAME } from "./shared.js";

export async function createDatabase(envDbConfig: EnvDbConfig): Promise<void> {
  const client = new pg.Client({
    host: envDbConfig.POSTGRES_HOST,
    port: envDbConfig.POSTGRES_PORT,
    user: envDbConfig.POSTGRES_USER,
    password: envDbConfig.POSTGRES_PASSWORD,
    database: POSTGRES_DB_NAME,
  });

  try {
    await client.connect();
    await runQuery(client, envDbConfig.POSTGRES_DB_NAME);
  } catch (err) {
    console.error("Creating db", err);
  } finally {
    await client.end();
  }
}

async function runQuery(client: Client, dbName: string) {
  const res = await client.query<{ datname: string }>(
    "SELECT datname FROM pg_catalog.pg_database WHERE datname = $1",
    [dbName],
  );
  if (res.rowCount === 0) {
    await client.query(`CREATE DATABASE ${dbName}`);
  }

  console.log(`Database ${dbName} created or already exists.`);
}