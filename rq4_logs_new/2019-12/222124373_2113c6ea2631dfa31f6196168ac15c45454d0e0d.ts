import rateLimit from 'express-rate-limit';
import RedisStore from 'rate-limit-redis';
import Redis from 'redis';
import { RequestHandler } from 'express';

export function rateLimiter(): RequestHandler {
    return rateLimit({
        store: RedisStore({
            client: Redis.createClient({
                host: process.env.REDIS_HOST || 'localhost'
            }),
        }),
        windowMs: 15 * 60 * 1000,
        max: 100,
        message: JSON.stringify({
            message: 'Too many requests, please try again later.'
        })
    });
}