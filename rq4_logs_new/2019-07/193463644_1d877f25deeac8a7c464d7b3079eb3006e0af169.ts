import { MinLength, Matches, IsBoolean, IsNotEmpty, Min } from 'class-validator';
import { Expose, Type, Transform } from 'class-transformer';

export class Environment {
  @Matches(new RegExp('mysql://\\w+:\\w+@.+:\\d+/\\w+', 'g'))
  @MinLength(17)
  @Expose()
  DB_URL: string;

  @Matches(new RegExp('^[^\\/?%*:|"<>.]{1,64}$'))
  @Expose()
  DB_NAME: string;

  @IsBoolean()
  @Expose()
  @Transform(value => value == 'true')
  DB_SYNCHRONIZE: boolean;

  @Expose()
  @IsBoolean()
  @Transform(value => value == 'true')
  DB_LOGGING: boolean;

  @IsNotEmpty()
  @Expose()
  DB_ENTITIES: string;

  @Min(1000)
  @Expose()
  @Type(() => Number)
  PORT: number;

  constructor(
    DB_URL: string,
    DB_NAME: string,
    DB_SYNCHRONIZE: boolean,
    DB_LOGGING: boolean,
    DB_ENTITIES: string,
    PORT: number
  ) {
    this.DB_URL = DB_URL;
    this.DB_NAME = DB_NAME;
    this.DB_SYNCHRONIZE = DB_SYNCHRONIZE;
    this.DB_LOGGING = DB_LOGGING;
    this.DB_ENTITIES = DB_ENTITIES;
    this.PORT = PORT;
  }
}