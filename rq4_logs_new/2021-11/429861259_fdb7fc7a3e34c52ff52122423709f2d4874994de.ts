import Redis from 'ioredis';
import { env } from './env';
import { logger } from './logger';

export const redis = new Redis({
	port: 6379,
	host: '127.0.0.1',
	password: 'password123',
	connectTimeout: 5000,
	maxRetriesPerRequest: 3,
	showFriendlyErrorStack: env.NODE_ENV === 'development',
});

redis.on('connect', () => logger.info('Redis connection established!'));
redis.on('reconnecting', () => logger.info('reconnecting'));

redis.on('error', err => {
	if (err.name === 'MaxRetriesPerRequestError') {
		logger.error(`Critical Redis error: ${err.message}. Shutting down.`);
		process.exit(1);
	} else {
		logger.error(`Redis encountered an error: ${err.message}.`);
		process.exit(1);
	}
});