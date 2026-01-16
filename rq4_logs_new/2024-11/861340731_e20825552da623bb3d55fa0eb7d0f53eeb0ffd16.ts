import { Module, DynamicModule, Global } from '@nestjs/common';
import { RedisConfig } from './dtos/redis-creation.dto';
import { IOREDIS, REDIS_CLIENT } from './redis.constant';
import { RedisHelper } from './service/redis.service';
import { Type } from '@nestjs/common/interfaces/type.interface';
import { ForwardReference } from '@nestjs/common/interfaces/modules/forward-reference.interface';
import { Provider } from '@nestjs/common/interfaces/modules/provider.interface';
import Redis from 'ioredis';

@Global()
@Module({})
export class RedisModule {
  static register(redisConfig: RedisConfig): DynamicModule {
    const { host, port, accessKey, isPublic } = redisConfig;

    const imports: Array<Type<any> | DynamicModule | ForwardReference> = [];
    const providers: Provider[] = [];
    const exports: Provider[] = [];

    if (host && port && accessKey) {
      const redisInstance = new Redis({
        host,
        port,
        password: accessKey,
        tls: {},
        retryStrategy(times) {
          return Math.min(times * 50, 2000);
        },
      });

      providers.push({
        provide: IOREDIS,
        useValue: redisInstance,
      });
      providers.push(RedisHelper);
      exports.push({
        provide: IOREDIS,
        useValue: redisInstance,
      });
      exports.push(RedisHelper);
    }

    return {
      module: RedisModule,
      imports,
      providers,
      exports,
      global: !!isPublic,
    };
  }
}