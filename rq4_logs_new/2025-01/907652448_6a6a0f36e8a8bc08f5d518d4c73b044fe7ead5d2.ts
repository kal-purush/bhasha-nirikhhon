import { TypedRoute } from '@nestia/core';
import { Controller, Inject } from '@nestjs/common';

import { JwtPayload } from '../../../../core/auth';
import { IGetDdaysByUserIdService } from '../../../../core/dday';
import { CurrentUser } from '../../../shared/decorators/current-user.decorator';
import { GetDdaysByUserIdService } from '../../services';
import { IGetDdaysResDto } from './i-get-ddays.res.dto';

@Controller()
export class GetMyDdaysController {
  constructor(
    @Inject(GetDdaysByUserIdService)
    private readonly getDdaysByUserIdService: IGetDdaysByUserIdService,
  ) {}

  @TypedRoute.Get()
  public async execute(
    @CurrentUser() user: JwtPayload,
  ): Promise<IGetDdaysResDto> {
    const ddays = await this.getDdaysByUserIdService.execute(user.userId);

    return {
      ddays,
    };
  }
}