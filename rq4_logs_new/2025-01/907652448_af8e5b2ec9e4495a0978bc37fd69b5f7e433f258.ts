import { TypedRoute } from '@nestia/core';
import { Controller, HttpCode, HttpStatus, Inject } from '@nestjs/common';

import {
  ICancelAccountByUserIdService,
  JwtPayload,
} from '../../../../core/auth';
import { CurrentUser } from '../../../shared/decorators';
import { CancelAccountByUserIdService } from '../../services';

@Controller()
export class CancelAccountController {
  constructor(
    @Inject(CancelAccountByUserIdService)
    private readonly cancelAccountByUserIdService: ICancelAccountByUserIdService,
  ) {}

  @TypedRoute.Delete()
  @HttpCode(HttpStatus.NO_CONTENT)
  public async execute(@CurrentUser() user: JwtPayload): Promise<void> {
    await this.cancelAccountByUserIdService.execute(user.userId);
  }
}