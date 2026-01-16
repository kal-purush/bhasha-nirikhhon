import { drizzle } from "drizzle-orm/postgres-js";
import { migrate } from "drizzle-orm/postgres-js/migrator";
import drizzleConfig from "drizzle.config";
import postgres from "postgres";
import { env } from "~/env.mjs";

// for migrations
const migrationClient = postgres(env.DATABASE_URL, { max: 1 });

migrate(drizzle(migrationClient, { logger: env.DATABASE_VERBOSE }), {
  migrationsFolder: drizzleConfig.out!,
})
  .then(() => {
    // eslint-disable-next-line no-console
    console.log("Migration complete");
    process.exit(0);
  })
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });