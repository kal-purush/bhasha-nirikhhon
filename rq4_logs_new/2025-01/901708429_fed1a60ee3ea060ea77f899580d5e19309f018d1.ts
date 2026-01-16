/* eslint-disable @typescript-eslint/explicit-function-return-type */
import { Injectable, NestMiddleware } from '@nestjs/common';
import { Request, Response, NextFunction } from 'express';
import rateLimit from 'express-rate-limit';
import { RedisStore } from 'rate-limit-redis';
import { redisClient } from '@/config/redis.config';

interface RateLimitConfig {
	windowMs: number;
	max: number;
}

@Injectable()
export class RateLimitMiddlewareFactory {
	static create(config: RateLimitConfig): new () => NestMiddleware {
		@Injectable()
		class CustomRateLimit implements NestMiddleware {
			private limiter = rateLimit({
				windowMs: config.windowMs || 15 * 60 * 1000,
				max: config.max || 100,
				message: {
					status: 429,
					error: 'Too many requests',
					message: 'Too many requests from this IP, please try again later.',
				},
				standardHeaders: true,
				legacyHeaders: false,
				store: new RedisStore({
					sendCommand: (...args: string[]) => redisClient.sendCommand(args),
				}),
			});

			use(req: Request, res: Response, next: NextFunction) {
				this.limiter(req, res, next);
			}
		}
		return CustomRateLimit;
	}
}