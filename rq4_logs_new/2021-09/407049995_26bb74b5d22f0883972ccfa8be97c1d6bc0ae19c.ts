import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';

import { User } from './user/entities/user.entity';
import { Playlist } from './playlist/entities/playlist.entity';
import { Event } from './event/entities/event.entity';
import { PlaylistUnit } from './playlist/entities/playlist.unit.entity';
import { Content } from './content/entities/content.entity';
import { Display } from './display/entities/display.entity';
@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: process.env.DB_HOST || 'localhost',
      port: +process.env.DB_PORT || 3306,
      username: process.env.DB_USERNAME || 'test',
      password: process.env.DB_PASSWORD || 'abc123',
      database: process.env.DB_BASE_NAME || 'test',
			entities: [User,Playlist, Event, PlaylistUnit, Content, Display],
      synchronize: true,
    }),
  ],
})
export class DataBaseModule {}