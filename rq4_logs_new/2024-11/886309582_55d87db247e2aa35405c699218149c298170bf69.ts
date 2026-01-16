import { Pool } from 'pg';

export const pool = new Pool({
  user: 't-pr',
  host: 'localhost',
  database: 't-pr',
  password: 't-pr',
  port: 5432,
});