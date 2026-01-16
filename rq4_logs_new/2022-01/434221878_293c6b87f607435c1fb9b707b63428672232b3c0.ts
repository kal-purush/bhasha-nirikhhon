// config.ts

import { ConfigService, registerAs } from '@nestjs/config';

import { User } from './../users/entities/user.entity';

export default registerAs('database', async () => {
  const configService = new ConfigService();
  return {
    type: 'postgres',
    host: configService.get('PGHOST'),
    port: configService.get<number>('PGPORT'),
    username: configService.get('POSTGRES_USER'),
    password: configService.get('POSTGRES_PASSWORD'),
    database: configService.get('POSTGRES_DB'),
    entities: [User],
    autoLoadEntities: true,
    migrations: ['dist/db/migrations/*.js'],
    cli: {
      migrationsDir: 'src/db/migrations',
    },
    migrationsRun: true,
    synchronize: false,
    logging: true,
  };
});
//__EOF__