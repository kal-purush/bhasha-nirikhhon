import { TypedQuery, TypedRoute } from '@nestia/core';
import { Controller, Inject } from '@nestjs/common';

import { JwtPayload } from '../../../../core/auth';
import {
  GetStudyRecordByDateServiceInput,
  IGetStudyRecordByDateService,
} from '../../../../core/study-record';
import { CurrentUser } from '../../../shared/decorators';
import { GetStudyRecordByDateService } from '../../services';
import { IGetStudyRecordByDateReqDto } from './i-get-study-record-by-date.req.dto';
import { IGetStudyRecordByDateResDto } from './i-get-study-record-by-date.res.dto';

@Controller()
export class GetStudyRecordByDateController {
  constructor(
    @Inject(GetStudyRecordByDateService)
    private readonly getStudyRecordByDateService: IGetStudyRecordByDateService,
  ) {}

  @TypedRoute.Get()
  public async execute(
    @CurrentUser() user: JwtPayload,
    @TypedQuery() query: IGetStudyRecordByDateReqDto,
  ): Promise<IGetStudyRecordByDateResDto | null> {
    return await this.getStudyRecordByDateService.execute(
      new GetStudyRecordByDateServiceInput(
        query.date,
        query.userId,
        user.userId,
      ),
    );
  }
}