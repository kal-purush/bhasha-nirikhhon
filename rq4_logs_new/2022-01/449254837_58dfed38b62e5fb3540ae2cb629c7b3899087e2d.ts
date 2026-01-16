import { IsBoolean, IsOptional, IsString } from 'class-validator';

export class GetTaskMetadataDto {
  @IsString()
  @IsOptional()
  details?: string;

  @IsOptional()
  @IsBoolean()
  isDeactivated?: boolean;
}