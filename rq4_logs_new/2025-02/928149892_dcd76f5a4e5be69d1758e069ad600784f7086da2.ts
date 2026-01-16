export const config = {
  PROJECT_NAME: process.env.PROJECT_NAME || 'svc-gateway-api',
  LOG_LEVEL: process.env.LOG_LEVEL || 'fatal',
  PORT: parseInt(process.env.PORT || '3030', 10),
  JWT: {
    SECRET: process.env.JWT_SECRET || 'jwt_secret',
    EXPIRATION: process.env.JWT_EXPIRATION || '3h',
  },
  MAX_LOGIN_ATTEMPTS: parseInt(process.env.MAX_LOGIN_ATTEMPTS || '5'),
  MAX_DEVICE_SESSIONS: parseInt(process.env.MAX_DEVICE_SESSIONS || '5'),
  THROTTLE_TTL: parseInt(process.env.THROTTLE_TTL || '1'),
  THROTTLE_LIMIT: parseInt(process.env.THROTTLE_LIMIT || '5'),
};