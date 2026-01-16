/* eslint-disable @typescript-eslint/explicit-function-return-type */
/* eslint-disable @typescript-eslint/explicit-module-boundary-types */
import { Injectable } from '@nestjs/common';
import {
	HealthCheckService,
	DiskHealthIndicator,
	MemoryHealthIndicator,
	TypeOrmHealthIndicator,
	HealthIndicatorResult,
} from '@nestjs/terminus';
import { redisClient } from '@/config/redis.config';

@Injectable()
export class HealthService {
	constructor(
		private health: HealthCheckService,
		private db: TypeOrmHealthIndicator,
		private memory: MemoryHealthIndicator,
		private disk: DiskHealthIndicator,
	) {}

	async checkDatabase(): Promise<HealthIndicatorResult> {
		return this.db.pingCheck('database', { timeout: 300 });
	}

	async checkMemoryHeap(): Promise<HealthIndicatorResult> {
		return this.memory.checkHeap('memory_heap', 150 * 1024 * 1024);
	}

	async checkMemoryRSS(): Promise<HealthIndicatorResult> {
		return this.memory.checkRSS('memory_rss', 150 * 1024 * 1024);
	}

	async checkDiskStorage(): Promise<HealthIndicatorResult> {
		return this.disk.checkStorage('storage', { thresholdPercent: 0.8, path: '/' });
	}

	async checkRedis(): Promise<HealthIndicatorResult> {
		try {
			const pingResult = await redisClient.ping();
			if (pingResult === 'PONG') {
				return { redis: { status: 'up' } };
			}
			throw new Error('Ping failed');
		} catch (error) {
			return { redis: { status: 'down', error: error.message } };
		}
	}

	async performHealthChecks() {
		return this.health.check([
			() => this.checkDatabase(),
			() => this.checkMemoryHeap(),
			() => this.checkMemoryRSS(),
			() => this.checkDiskStorage(),
			() => this.checkRedis(),
		]);
	}
}