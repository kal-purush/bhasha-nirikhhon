import { IsEmail, IsString, IsNotEmpty, MinLength } from 'class-validator';

export class UpdateUserDto {
  @IsEmail()
  email: string;

  @IsString()
  @IsNotEmpty()
  name: string;

  bio: string;
}

export default UpdateUserDto;