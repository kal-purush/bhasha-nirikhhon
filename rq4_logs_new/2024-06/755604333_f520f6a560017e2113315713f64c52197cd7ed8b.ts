import {
  IsOptional,
  IsString,
  IsBoolean,
  IsInt,
  Min,
  Max,
  IsEmail,
} from 'class-validator';
import { Expose, Transform } from 'class-transformer';

/**
 * Class representing mail-related environment variables.
 */
export class MailVariables {
  @Expose()
  @IsOptional()
  @IsString()
  MAIL_SERVICE?: string;

  @Expose()
  @IsOptional()
  @IsString()
  MAIL_HOST?: string;

  @Expose()
  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(65535)
  MAIL_PORT?: number;

  @Expose()
  @IsOptional()
  @IsBoolean()
  @Transform(({ obj }) => obj.MAIL_SECURE === 'true')
  MAIL_SECURE?: boolean;

  @Expose()
  @IsEmail()
  MAIL_USER: string;

  @Expose()
  @IsString()
  MAIL_PASSWORD: string;
}