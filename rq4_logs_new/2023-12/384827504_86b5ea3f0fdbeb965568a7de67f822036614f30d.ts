import { Module } from '@nestjs/common';
import {
  ClientOptions,
  ClientProxyFactory,
  Transport,
} from '@nestjs/microservices';
import { RolesGuard } from '../../guards/applicationRoles.guard';
import { MyLoggerService } from '../logger/logger.service';
import { UserController } from './user.controller';
import { UserService } from './user.service';
import { USER_SERVICE, APP_GUARD, ENV } from '@app/shared-modules';
import { ConfigModule, ConfigService } from '@nestjs/config';

@Module({
  imports: [
    ConfigModule.forRoot({
      envFilePath: ['./.env.development.local'],
      isGlobal: true,
      cache: true,
    }),
  ],
  controllers: [UserController],
  providers: [
    {
      provide: APP_GUARD,
      useClass: RolesGuard,
    },
    {
      provide: USER_SERVICE,
      useFactory: (configService: ConfigService) => {
        const env = configService.get('NODE_ENV');
        const host =
          env === ENV.DEVELOPMENT
            ? configService.get('USER_SERVICE_HOST')
            : 'localhost';
        const port =
          env === ENV.DEVELOPMENT
            ? configService.get('USER_SERVICE_PORT')
            : 8080;
        const options: ClientOptions = {
          transport: Transport.TCP,
          options: {
            host,
            port,
          },
        };
        return ClientProxyFactory.create(options);
      },
      inject: [ConfigService],
    },
    UserService,
    MyLoggerService,
  ],
})
export class UsersModule {}