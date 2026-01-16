import { TypedRoute } from '@nestia/core';
import {
  Controller,
  HttpCode,
  HttpStatus,
  Inject,
  UseGuards,
} from '@nestjs/common';
import { ConfigType } from '@nestjs/config';

import { IJwtTokenProvider, JwtPayload } from '../../../../core/auth';
import { tokenEnv } from '../../../app-config/envs';
import { Public } from '../../../shared/decorators';
import { CurrentUser } from '../../../shared/decorators/current-user.decorator';
import { RefreshTokenGuard } from '../../guards';
import { JwtTokenProvider } from '../../providers';
import { IRefreshTokenResDto } from './i-refresh-token.res.dto';

@Controller()
export class RefreshTokenController {
  constructor(
    @Inject(tokenEnv.KEY)
    private readonly tokenConfig: ConfigType<typeof tokenEnv>,
    @Inject(JwtTokenProvider)
    private readonly jwtTokenProvider: IJwtTokenProvider,
  ) {}

  @Public()
  @UseGuards(RefreshTokenGuard)
  @HttpCode(HttpStatus.OK)
  @TypedRoute.Get('/refresh')
  public async execute(
    @CurrentUser() user: JwtPayload,
  ): Promise<IRefreshTokenResDto> {
    const expiresAt =
      Math.floor(Date.now() / 1000) + this.tokenConfig.jwtExpiresInSeconds;
    const accessToken = await this.jwtTokenProvider.sign({
      userId: user.userId,
    });

    return {
      accessToken,
      expiresAt,
    };
  }
}