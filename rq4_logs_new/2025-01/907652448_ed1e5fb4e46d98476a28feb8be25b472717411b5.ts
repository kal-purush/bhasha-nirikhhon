import { TypedParam, TypedRoute } from '@nestia/core';
import { Controller, HttpCode, HttpStatus, Inject } from '@nestjs/common';

import { JwtPayload } from '../../../../core/auth';
import {
  DeleteDdayServiceInput,
  IDeleteDdayByDdayIdService,
} from '../../../../core/dday';
import { CurrentUser } from '../../../shared/decorators';
import { DeleteDdayByDdayIdService } from '../../services';

@Controller()
export class DeleteDdayController {
  constructor(
    @Inject(DeleteDdayByDdayIdService)
    private readonly deleteDdayByDdayIdService: IDeleteDdayByDdayIdService,
  ) {}

  @TypedRoute.Delete(':ddayId')
  @HttpCode(HttpStatus.NO_CONTENT)
  public async execute(
    @CurrentUser() user: JwtPayload,
    @TypedParam('ddayId') ddayId: string,
  ): Promise<void> {
    await this.deleteDdayByDdayIdService.execute(
      new DeleteDdayServiceInput(ddayId, user.userId),
    );
  }
}