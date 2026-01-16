// ormconfig.ts

import dotenv from 'dotenv';
import path from 'path';

dotenv.config({
  path: path.join(__dirname, '.env'),
});

export const { POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, PGHOST, PGPORT } =
  process.env;

export default {
  type: 'postgres',
  host: PGHOST,
  port: PGPORT,
  username: POSTGRES_USER,
  password: POSTGRES_PASSWORD,
  database: POSTGRES_DB,
  migrations: ['migration/*.ts'],
  cli: {
    /* entitiesDir: 'entity', */
    migrationsDir: 'migration',
    /* subscribersDir: 'subscriber', */
  },
  logging: true,
};

//__EOF__