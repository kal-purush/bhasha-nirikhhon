import { Module, Global } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { redisStore } from 'cache-manager-redis-yet';
import { EnvVariables } from '../constants';
import {
  CacheInterceptor,
  CacheModule,
  CacheStore,
} from '@nestjs/cache-manager';

@Global()
@Module({
  imports: [
    CacheModule.registerAsync({
      imports: [ConfigModule],
      useFactory: async (configService: ConfigService) => {
        const store = await redisStore({
          socket: {
            host: configService.get<string>(EnvVariables.REDIS_HOST),
            port: configService.get<number>(EnvVariables.REDIS_PORT),
          },
          username: configService.get<string>(EnvVariables.REDIS_USERNAME),
          password: configService.get<string>(EnvVariables.REDIS_PASSWORD),
        });

        return {
          store: store as unknown as CacheStore,
          ttl: 5 * 60 * 1000,
        };
      },
      inject: [ConfigService],
    }),
  ],
  providers: [CacheInterceptor],
  exports: [CacheModule, CacheInterceptor],
})
export class GlobalCacheModule {}