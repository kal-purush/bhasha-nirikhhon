import {
  Body,
  Controller,
  ForbiddenException,
  Post,
  Req,
  Res,
  UnauthorizedException,
  UseGuards,
} from '@nestjs/common';
import { Request, Response } from 'express';
import { AuthService } from 'src/auth/auth.service';
import { JwtAuthGuard } from 'src/auth/guard/jwt-auth.guard';
import { User } from 'src/user/entity/user.entity';
import { OTPAuthGuard } from './guard/otp-auth.guard';
import { OTPService } from './otp.service';

@Controller('otp')
export class OTPController {
  constructor(
    private otpService: OTPService,
    private authService: AuthService,
  ) {}

  @UseGuards(JwtAuthGuard)
  @Post('register')
  async register(
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    return this.otpService.register(req.user as User);
  }

  @UseGuards(JwtAuthGuard)
  @Post('deregister')
  async deregister(
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    return this.otpService.deregister(req.user as User);
  }

  @UseGuards(OTPAuthGuard)
  @Post('login')
  async login(
    @Req() req: Request,
    @Body('token') token: string,
    @Res({ passthrough: true }) res: Response,
  ) {
    if (this.otpService.login(req.user as User, token)) {
      const jwtToken = await this.authService.sign(req.user as User, true);
      res.cookie('access_token', jwtToken);
    } else {
      throw new UnauthorizedException();
    }
  }
}