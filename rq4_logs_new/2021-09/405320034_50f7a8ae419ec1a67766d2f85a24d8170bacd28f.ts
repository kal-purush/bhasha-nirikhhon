import { BadRequestException, Body, Controller, Post } from '@nestjs/common';
import { CreateUserDto } from 'src/users/dto/create-user.dto';
import { UserLoginDto } from 'src/users/dto/user-login.dto';
import { UsersService } from 'src/users/users.service';
import { AuthService } from './auth.service';
import { LoginStatus } from './interfaces/login-status.interface';
import { RegistrationStatus } from './interfaces/registration-status.interface';

@Controller()
export class AuthController {
  constructor(private readonly authService: AuthService) {}

  @Post('register')
  async register(
    @Body() createUserDto: CreateUserDto,
  ): Promise<RegistrationStatus> {
    const result: RegistrationStatus = await this.authService.register(
      createUserDto,
    );

    if (!result.success) {
      throw new BadRequestException(result.message);
    }
    return result;
  }

  @Post('login')
  async login(@Body() userLoginDto: UserLoginDto): Promise<LoginStatus> {
    return await this.authService.login(userLoginDto);
  }
}