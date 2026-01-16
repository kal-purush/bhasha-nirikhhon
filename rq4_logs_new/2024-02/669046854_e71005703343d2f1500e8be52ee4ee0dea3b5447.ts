import * as dotenv from 'dotenv';

const env = process.env.NODE_ENV || 'development';

if (env === 'production') {
  dotenv.config({ path: '.env.prod' });
} else {
  dotenv.config({ path: '.env.dev' });
}

export const CORS_ORIGIN_Domain_1: string = process.env.CORS_URL_1 || '';
export const CORS_ORIGIN_Domain_2: string = process.env.CORS_URL_2 || '';

export default () => ({
  port: process.env.PORT || 5050,
  host: process.env.HOST || 'localhost',
  db: process.env.DATABASE_URL,
  sessionSecret: process.env.SESSION_SECRET || 'keyboard cat',
  jwtSecretKey: process.env.JWT_SECRET_KEY || 'SECRET_KEY',
  jwtExpirationTime: process.env.JWT_EXPIRATION_TIME || '1h',
  cookieName: process.env.COOKIE_NAME || 'jwt',
});