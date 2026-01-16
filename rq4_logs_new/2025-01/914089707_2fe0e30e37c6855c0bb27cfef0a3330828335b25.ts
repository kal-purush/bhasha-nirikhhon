import { DataSource } from 'typeorm';
import { ConfigModule } from '@nestjs/config';
import { UserEntity } from './user/entities/user.entity';
import { MissionEntity } from './mission/entities/mission.entity';
import { UserMissionEntity } from './mission/entities/user-missions.entity';

ConfigModule.forRoot({
  envFilePath: '.env',
});

export const AppDataSource = new DataSource({
  type: process.env.DB_TYPE as 'mariadb',
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT) || 3307,
  username: process.env.DB_USERNAME,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
  entities: [UserEntity, MissionEntity, UserMissionEntity],
  synchronize: true,
  logging: true,
});

export default AppDataSource;