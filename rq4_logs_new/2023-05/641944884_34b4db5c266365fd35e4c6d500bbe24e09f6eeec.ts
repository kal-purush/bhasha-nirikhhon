import { ApiPropertyOptional } from '@nestjs/swagger';
import { IsOptional, IsString, MaxLength } from 'class-validator';

export class CreateOrganizationDto {
  @MaxLength(100000)
  @IsString()
  @IsOptional()
  @ApiPropertyOptional({ maxLength: 100000 })
  logo: string;

  @MaxLength(100000)
  @IsString()
  @ApiPropertyOptional({ maxLength: 100000 })
  name: string;

  @MaxLength(100000)
  @IsString()
  @IsOptional()
  @ApiPropertyOptional({ maxLength: 100000 })
  address: string;

  @MaxLength(100000)
  @IsString()
  @IsOptional()
  @ApiPropertyOptional({ maxLength: 100000 })
  phone: string;

  @MaxLength(100000)
  @IsString()
  @IsOptional()
  @ApiPropertyOptional({ maxLength: 100000 })
  email: string;
}