import { TypedParam, TypedRoute } from '@nestia/core';
import { Controller, Inject } from '@nestjs/common';

import { JwtPayload } from '../../../../core/auth';
import {
  GetSubjectsByUserIdServiceInput,
  IGetSubjectsByUserIdService,
} from '../../../../core/subject';
import { CurrentUser } from '../../../shared/decorators';
import { GetSubjectsByUserIdService } from '../../services';
import { IGetSubjectsByUserIdResDto } from './i-get-subjects-by-user-id.res.dto';

@Controller()
export class GetSubjectsByUserIdController {
  constructor(
    @Inject(GetSubjectsByUserIdService)
    private readonly getSubjectsByUserIdService: IGetSubjectsByUserIdService,
  ) {}

  @TypedRoute.Get(':userId')
  public async execute(
    @CurrentUser() user: JwtPayload,
    @TypedParam('userId') userId: number,
  ): Promise<IGetSubjectsByUserIdResDto> {
    const subjects = await this.getSubjectsByUserIdService.execute(
      new GetSubjectsByUserIdServiceInput(userId, user.userId),
    );

    return { subjects };
  }
}