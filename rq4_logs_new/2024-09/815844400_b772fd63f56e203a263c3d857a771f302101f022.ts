import { Inject, Injectable } from '@nestjs/common';
import { Redis } from 'ioredis';

@Injectable()
export class RedisCacheService {
	constructor(@Inject('REDIS_CACHE_CLIENT') private readonly redisClient: Redis) {}

	async getCache(key: string) {
		return this.redisClient.get(key);
	}

	async setCache(key: string, value: string) {
		await this.redisClient.set(key, value);
	}

	async deleteCache(key: string) {
		await this.redisClient.del(key);
	}

	async pushQueue(key: string, value: string) {
		return await this.redisClient.rpush(key, value);
	}

	async getList(key: string) {
		return await this.redisClient.lrange(key, 0, -1);
	}
}