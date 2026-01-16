import { TypedBody, TypedParam, TypedRoute } from '@nestia/core';
import { BadRequestException, Controller, Inject } from '@nestjs/common';

import { JwtPayload } from '../../../../core/auth';
import {
  IUpdateDdayService,
  UpdateDdayServiceInput,
} from '../../../../core/dday/i-services/i-update-dday.service';
import { CurrentUser } from '../../../shared/decorators/current-user.decorator';
import { UpdateDdayService } from '../../services';
import { IUpdateDdayReqDto } from './i-update-dday.req.dto';

@Controller()
export class UpdateDdayController {
  constructor(
    @Inject(UpdateDdayService)
    private readonly updateDdayService: IUpdateDdayService,
  ) {}

  @TypedRoute.Patch(':ddayId')
  public async execute(
    @CurrentUser() user: JwtPayload,
    @TypedParam('ddayId') ddayId: string,
    @TypedBody()
    body: IUpdateDdayReqDto,
  ): Promise<void> {
    if (ddayId !== body.ddayId) {
      throw new BadRequestException();
    }

    await this.updateDdayService.execute(
      new UpdateDdayServiceInput(
        ddayId,
        user.userId,
        body.title,
        body.color,
        body.targetDatetime,
      ),
    );
  }
}