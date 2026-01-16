import { BULL_CONFIG_OPTION, BullOptions } from './pool.constant';
import { IOREDIS, REDIS_CONFIG } from '../redis/redis.constant';
import { RedisConfig } from '../redis/dtos/redis-creation.dto';

export const poolConfig = {
  provide: BULL_CONFIG_OPTION,
  inject: [IOREDIS, REDIS_CONFIG],
  useFactory: (redisConfig: RedisConfig): BullOptions => {
    return {
      connectionOption: {
        host: redisConfig.host,
        port: redisConfig.port,
        password: redisConfig.accessKey,
        tls: {},
        retryStrategy(times) {
          return Math.min(times * 50, 2000);
        },
      },
      defaultConnectionOptions: {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 5000,
        },
      },
    };
  },
};