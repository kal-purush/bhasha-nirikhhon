import { Controller, Post, Req, UseGuards } from '@nestjs/common';
import { UserType } from 'src/user/dto/user.dto';
import { AuthService } from './auth.service';
import { LocalAuthGuard } from './guards/local-auth.guard';
import { Request } from 'express';

@Controller('auth')
export class AuthController {
  constructor(private readonly authService: AuthService) {}
  @UseGuards(LocalAuthGuard)
  @Post('login')
  async login(@Req() req: Request): Promise<{ access_token: string }> {
    return await this.authService.login(req.user as UserType);
  }
}