import { TypedBody, TypedRoute } from '@nestia/core';
import { Controller, Inject } from '@nestjs/common';

import { JwtPayload } from '../../../../core/auth';
import {
  CreateDdayServiceInput,
  ICreateDdayService,
} from '../../../../core/dday';
import { CurrentUser } from '../../../shared/decorators/current-user.decorator';
import { CreateDdayService } from '../../services';
import { ICreateDdayReqDto } from './i-create-dday.req.dto';
import { ICreateDdayResDto } from './i-create-dday.res.dto';

@Controller()
export class CreateDdayController {
  constructor(
    @Inject(CreateDdayService)
    private readonly createDdayService: ICreateDdayService,
  ) {}

  @TypedRoute.Post()
  public async execute(
    @CurrentUser() user: JwtPayload,
    @TypedBody() body: ICreateDdayReqDto,
  ): Promise<ICreateDdayResDto> {
    const newDday = await this.createDdayService.execute(
      new CreateDdayServiceInput(
        user.userId,
        body.title,
        body.color,
        body.targetDatetime,
      ),
    );

    return {
      ddayId: newDday.ddayId,
    };
  }
}