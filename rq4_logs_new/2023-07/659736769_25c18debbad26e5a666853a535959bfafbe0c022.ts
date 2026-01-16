import {
  Body,
  Controller,
  Get,
  Post,
  Request,
  UseGuards,
} from '@nestjs/common';
import { UserAuthLogin } from '@application/use-cases/auth/auth-login';
import { LocalAuthGuard } from '@application/use-cases/auth/guards/local-auth.guard';
import { AuthRefreshToken } from '@application/use-cases/auth/auth-refresh-token';
import { RefreshJwtGuard } from '@application/use-cases/auth/guards/refresh-jwt-auth.guard';

@Controller('auth/')
export class UserAuthController {
  constructor(
    private userAuthLogin: UserAuthLogin,
    private authRefreshToken: AuthRefreshToken,
  ) {}

  @UseGuards(LocalAuthGuard)
  @Post('login')
  async login(@Request() req) {
    return await this.userAuthLogin.execute(req.user);
  }

  @UseGuards(RefreshJwtGuard)
  @Post('refresh')
  async refreshToken(@Request() req) {
    return await this.authRefreshToken.execute(req.user);
  }
}