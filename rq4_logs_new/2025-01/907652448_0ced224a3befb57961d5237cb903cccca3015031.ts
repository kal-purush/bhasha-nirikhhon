import { TypedRoute } from '@nestia/core';
import { Controller, Inject } from '@nestjs/common';

import { JwtPayload } from '../../../../core/auth';
import { IGetUserInfoService } from '../../../../core/user';
import { CurrentUser } from '../../../shared/decorators';
import { GetUserInfoService } from '../../services';
import { IGetUserInfoResDto } from './i-get-user-info.res.dto';

@Controller()
export class GetUserInfoController {
  constructor(
    @Inject(GetUserInfoService)
    private readonly getUserInfoService: IGetUserInfoService,
  ) {}

  @TypedRoute.Get()
  public async execute(
    @CurrentUser() user: JwtPayload,
  ): Promise<IGetUserInfoResDto> {
    const userDomain = await this.getUserInfoService.execute(user.userId);

    return {
      nickname: userDomain.nickname,
    };
  }
}