import { Inject, Injectable } from '@nestjs/common';
import { ConfigType } from '@nestjs/config';

import {
  IRefreshTokenProvider,
  VerifyRefreshTokenInput,
} from '../../../core/auth';
import {
  ICreateRefreshTokenRepository,
  IGetRefreshTokenRepository,
} from '../../../core/refresh-token';
import {
  CreateRefreshTokenRepository,
  GetRefreshTokenRepository,
} from '../../../infra/repositories';
import { tokenEnv } from '../../app-config/envs';
import { generateRandomString } from '../../shared/lib';

@Injectable()
export class RefreshTokenProvider implements IRefreshTokenProvider {
  constructor(
    @Inject(tokenEnv.KEY)
    private readonly tokenConfig: ConfigType<typeof tokenEnv>,
    @Inject(CreateRefreshTokenRepository)
    private readonly createRefreshTokenRepository: ICreateRefreshTokenRepository,
    @Inject(GetRefreshTokenRepository)
    private readonly getRefreshTokenRepository: IGetRefreshTokenRepository,
  ) {}

  public async generate(userId: number): Promise<string> {
    const token = generateRandomString(this.tokenConfig.refreshTokenLength);

    const expiresAt =
      Math.floor(Date.now() / 1000) +
      this.tokenConfig.refreshExpiresInDays * 3600 * 24;

    await this.createRefreshTokenRepository.execute({
      userId,
      token,
      expiresAt,
    });

    return token;
  }
  public async verify(payload: VerifyRefreshTokenInput): Promise<boolean> {
    const refreshToken = await this.getRefreshTokenRepository.execute(payload);

    return refreshToken !== null;
  }
}