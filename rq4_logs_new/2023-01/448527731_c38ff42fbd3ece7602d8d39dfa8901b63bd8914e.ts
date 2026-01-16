import { DataSource } from 'typeorm';
import { ConfigService } from '@nestjs/config';

import { RoleEntity } from 'src/entities';


const configService = new ConfigService();

export const AppDataSource = new DataSource({
    type: 'postgres',
    host: 'localhost',
    port: 5432,
    username: 'postgres',
    password: '123456',
    database: 'beer-app-db',
    entities: [__dirname + '/**/entities/*{.ts,.js}', RoleEntity],
    logging: true,
    synchronize: false,
    migrationsRun: false,
    migrations: [__dirname + '/migrations/**/*{.ts,.js}'],
    migrationsTableName: 'history'
});