import { Body, Controller, Post } from '@nestjs/common';
import { AuthService } from './auth.service';
import { LoginDto } from './dtos/login.dto';

import { ApiTags } from '@nestjs/swagger';
import { LocalStrategy } from './local.strategy';

@ApiTags('Login')
@Controller('login')
export class AuthController {
  constructor(
    private readonly authService: AuthService,
    private readonly localStrategy: LocalStrategy,
  ) {}

  @Post()
  async login(@Body() user: LoginDto) {
    await this.localStrategy.validate(user.email, user.password);

    return this.authService.login(user.email);
  }
}