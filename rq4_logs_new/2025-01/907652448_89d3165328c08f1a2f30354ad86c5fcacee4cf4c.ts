import { TypedQuery, TypedRoute } from '@nestia/core';
import { Controller, Inject } from '@nestjs/common';

import { JwtPayload } from '../../../../core/auth';
import {
  GetStudyRecordsByRangeServiceInput,
  IGetStudyRecordsByRangeService,
} from '../../../../core/study-record';
import { CurrentUser } from '../../../shared/decorators';
import { GetStudyRecordsByRangeService } from '../../services';
import { IGetStudyRecordsByRangeReqDto } from './i-get-study-records-by-range.req.dto';
import { IGetStudyRecordsByRangeResDto } from './i-get-study-records-by-range.res.dto';

@Controller()
export class GetStudyRecordsByRangeController {
  constructor(
    @Inject(GetStudyRecordsByRangeService)
    private readonly getStudyRecordsByRangeService: IGetStudyRecordsByRangeService,
  ) {}

  @TypedRoute.Get('range')
  public async execute(
    @CurrentUser() user: JwtPayload,
    @TypedQuery() query: IGetStudyRecordsByRangeReqDto,
  ): Promise<IGetStudyRecordsByRangeResDto> {
    const studyRecords = await this.getStudyRecordsByRangeService.execute(
      new GetStudyRecordsByRangeServiceInput(
        user.userId,
        query.userId,
        query.startDate,
        query.endDate,
      ),
    );

    return { studyRecords };
  }
}