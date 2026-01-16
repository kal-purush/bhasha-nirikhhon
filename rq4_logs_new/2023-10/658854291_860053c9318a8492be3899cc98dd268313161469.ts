import { ApiProperty } from '@nestjs/swagger';
import { IsNotEmpty, IsString, Matches, MinLength } from 'class-validator';

export class ChangePasswordDto {
  @ApiProperty()
  @IsString()
  @IsNotEmpty()
  oldPassword: string;

  @ApiProperty()
  @IsString()
  @IsNotEmpty()
  @MinLength(6)
  @Matches(/^(?=.*[A-Z])(?=.*[a-z])(?=.*\d)[A-Za-z\d]*$/, {
    message:
      'Password is too weak! It must contain at least one uppercase letter, one lowercase letter, and one number.',
  })
  newPassword: string;

  @ApiProperty()
  @IsString()
  @IsNotEmpty()
  confirmNewPassword: string;
}