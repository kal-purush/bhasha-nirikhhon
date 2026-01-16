import { Type } from "class-transformer";
import { IsOptional, IsString, ValidateNested } from "class-validator";

export class UpdateUserInfo {
  @IsString()
  @IsOptional()
  email?: string;

  @IsOptional()
  @IsString()
  bio?: string;

  @IsOptional()
  @IsString()
  image?: string;

  @IsOptional()
  @IsString()
  userName?: string;

  @IsOptional()
  @IsString()
  password?: string;
}

export class UpdateUserInput {
  @ValidateNested()
  @Type(() => UpdateUserInfo) // <-- Quan trọng để kích hoạt validation
  user!: UpdateUserInfo;
}