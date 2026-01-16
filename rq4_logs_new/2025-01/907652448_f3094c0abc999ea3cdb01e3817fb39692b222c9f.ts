import { TypedBody, TypedRoute } from '@nestia/core';
import { Controller, Inject } from '@nestjs/common';
import { ConfigType } from '@nestjs/config';

import {
  IAppleLoginService,
  IJwtTokenProvider,
  IRefreshTokenProvider,
} from '../../../../core/auth';
import { tokenEnv } from '../../../app-config/envs';
import { Public } from '../../../shared/decorators';
import { JwtTokenProvider, RefreshTokenProvider } from '../../providers';
import { AppleLoginService } from '../../services';
import { IGoogleLoginResDto } from '../google-login';
import { IAppleLoginReqDto } from './i-apple-login.req.dto';

@Controller()
export class AppleLoginController {
  constructor(
    @Inject(tokenEnv.KEY)
    private readonly tokenConfig: ConfigType<typeof tokenEnv>,
    @Inject(AppleLoginService)
    private readonly appleLoginService: IAppleLoginService,
    @Inject(JwtTokenProvider)
    private readonly jwtTokenProvider: IJwtTokenProvider,
    @Inject(RefreshTokenProvider)
    private readonly refreshTokenProvider: IRefreshTokenProvider,
  ) {}

  @Public()
  @TypedRoute.Post('apple')
  public async execute(
    @TypedBody() body: IAppleLoginReqDto,
  ): Promise<IGoogleLoginResDto> {
    const loginResult = await this.appleLoginService.execute(body);

    const expiresAt =
      Math.floor(Date.now() / 1000) + this.tokenConfig.jwtExpiresInSeconds;

    const accessToken = await this.jwtTokenProvider.sign({
      userId: loginResult.userId,
    });

    const refreshToken = await this.refreshTokenProvider.generate(
      loginResult.userId,
    );

    return {
      accessToken,
      refreshToken,
      expiresAt,
      isNewUser: loginResult.isNewUser,
      userId: loginResult.userId,
    };
  }
}