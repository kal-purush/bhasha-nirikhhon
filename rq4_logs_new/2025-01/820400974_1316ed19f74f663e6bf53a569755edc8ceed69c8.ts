import type { Config } from "drizzle-kit";

const { LOCAL_DB_PATH, DB_ID, D1_TOKEN, CLOUDFLARE_ACCOUNT_ID } = process.env;

export default LOCAL_DB_PATH
  ? ({
      schema: "./src/db/schema.ts",
      dialect: "sqlite",
      out: "./migrations",
      dbCredentials: {
        url: LOCAL_DB_PATH,
      },
    } satisfies Config)
  : ({
      schema: "./src/db/schema.ts",
      out: "./migrations",
      dialect: "sqlite",
      driver: "d1-http",
      dbCredentials: {
        databaseId: DB_ID!,
        token: D1_TOKEN!,
        accountId: CLOUDFLARE_ACCOUNT_ID!,
      },
    } satisfies Config);