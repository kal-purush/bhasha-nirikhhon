import pino from 'pino';

export const logger = pino({
    name: 'sls-movie-api',
    level: process.env.DEBUG_LEVEL || 'debug',
});