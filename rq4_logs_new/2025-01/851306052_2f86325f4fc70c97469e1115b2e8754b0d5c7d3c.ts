import {
  Logger,
  Module,
  // OnApplicationShutdown,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { InjectDataSource, TypeOrmModule } from '@nestjs/typeorm';
import { EnvironmentVariables } from './env.validation';
import { DataSource } from 'typeorm';
import { ApiConfigService } from './env.service';

@Module({
  imports: [
    TypeOrmModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService<EnvironmentVariables>) => ({
        type: 'postgres',
        host: configService.get('POSTGRES_HOST'),
        port: configService.get('POSTGRES_PORT'),
        username: configService.get('POSTGRES_USER'),
        password: configService.get('POSTGRES_PASSWORD'),
        database: configService.get('POSTGRES_DB'),
        entities: configService.,
        synchronize: true,
        applicationName: 'server',
        // logging: true,
      }),
    }),
  ],
  controllers: [],
  providers: [],
})
export class DBModule implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(DBModule.name);

  constructor(@InjectDataSource() private readonly dataSource: DataSource, private ApiConfigService: ApiConfigService) {
    this.logger.log(`DataSource created: ${this.dataSource}`);
    ApiConfigService.
  }

  async onModuleInit() {
    this.logger.log(
      `DataSource is initialized: ${this.dataSource.isInitialized}`,
    );
    if (this.dataSource.isInitialized) {
      this.logger.log('Database connection established successfully.');
    } else {
      try {
        await this.dataSource.initialize();
        this.logger.log('Database connection initialized successfully.');
      } catch (error) {
        this.logger.error(
          'Error while initializing database connection:',
          error.message,
        );
      }
    }
  }

  async onModuleDestroy(signal?: string) {
    this.logger.log(`Application is shutting down due to signal: ${signal}`);
    if (this.dataSource.isInitialized) {
      try {
        this.logger.log('Closing database connection...');
        await this.dataSource.destroy();
        this.logger.log('Database connection closed successfully.');
      } catch (error) {
        this.logger.error(
          'Error while closing database connection:',
          error.message,
        );
      }
    } else {
      this.logger.warn('DataSource is not initialized; skipping close.');
    }
  }
}