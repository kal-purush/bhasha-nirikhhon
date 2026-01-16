import { Pool } from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const pgPool = new Pool({
  connectionString: process.env.DATABASE_URL,
  application_name: 'michis_postgraphile',
  ssl: {
    // To avoid RDS connection errors
    rejectUnauthorized: false,
  },
});

export default pgPool;