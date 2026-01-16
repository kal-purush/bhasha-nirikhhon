import dotenv from 'dotenv';

dotenv.config();

export const dbConnection: any = {
  type: 'mariadb',
  url: process.env.URL,
  database: process.env.DATABASE,
  synchronize: process.env.SYNCHRONIZE,
  logging: process.env.LOGGING,
  entities: [process.env.ENTITIES],
  cli: {
    entitiesDir: 'entities'
  }
};

export const start = () => {
  if (
    process.env.URL &&
    process.env.DATABASE &&
    process.env.SYNCHRONIZE &&
    process.env.LOGGING &&
    process.env.ENTITIES
  ) {
    return true;
  } else {
    return false;
  }
};