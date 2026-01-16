import { IsNotEmpty, IsEmail, IsString, IsStrongPassword } from "class-validator";

export class LoginUserDto {
  @IsNotEmpty()
  @IsEmail()
  email: string;

  @IsString()
  @IsStrongPassword()
  password: string;
}