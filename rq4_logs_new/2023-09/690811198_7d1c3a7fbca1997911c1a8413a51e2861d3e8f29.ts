import {
  Body,
  Controller,
  HttpCode,
  HttpStatus,
  Post,
  Req,
  UseGuards,
} from '@nestjs/common';
import { AuthService } from './auth.service';
import { AuthDto } from './dto';
import { Tokens } from './types';
import { Request } from 'express';
import { AtGuard, RtGuard } from 'src/common/guards';
import { UserId } from 'src/users/user.entity';
import {
  GetCurrentUser,
  GetCurrentUserId,
  PublicEndpoint,
} from 'src/common/decorators';
import { Public } from '@prisma/client/runtime/library';

@Controller('auth')
export class AuthController {
  constructor(private authService: AuthService) {}

  @PublicEndpoint()
  @Post('local/signup')
  @HttpCode(HttpStatus.CREATED)
  signupLocal(@Body() dto: AuthDto): Promise<Tokens> {
    return this.authService.signupLocal(dto);
  }

  @PublicEndpoint()
  @Post('local/signin')
  @HttpCode(HttpStatus.OK)
  signinLocal(@Body() dto: AuthDto): Promise<Tokens> {
    return this.authService.signinLocal(dto);
  }

  @PublicEndpoint()
  @Post('logout')
  @HttpCode(HttpStatus.OK)
  logout(@GetCurrentUserId() userId: UserId) {
    return this.authService.logout(userId);
  }

  @UseGuards(RtGuard)
  @Post('/refresh')
  @HttpCode(HttpStatus.OK)
  refresh(
    @GetCurrentUser('refreshToken') refreshToken: string,
    @GetCurrentUserId() userId: UserId,
  ) {
    return this.authService.refresh(userId, refreshToken);
  }
}