import { DataSource } from 'typeorm';
import * as dotenv from 'dotenv';
import * as dotenvExpand from 'dotenv-expand';

// load environment variables
let env = dotenv.config();
dotenvExpand.expand(env);

export const dbDataSource = new DataSource({
  type: 'postgres',
  host: process.env['DB_HOST'],
  port: process.env['DB_PORT'] ? +process.env['DB_PORT'] : 5432,
  username: process.env['DB_USER'],
  password: process.env['DB_PASS'],
  database: process.env['DB_NAME'],
  entities: [`dist/**/*.entity.js`],
  migrationsRun: false,
  synchronize: false,
});