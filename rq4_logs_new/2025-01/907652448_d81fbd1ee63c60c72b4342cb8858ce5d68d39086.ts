import { TypedBody, TypedRoute } from '@nestia/core';
import { Controller, HttpCode, HttpStatus, Inject } from '@nestjs/common';

import { JwtPayload } from '../../../../core/auth';
import {
  IUpdateUserService,
  UpdateUserServiceInput,
} from '../../../../core/user';
import { CurrentUser } from '../../../shared/decorators';
import { UpdateUserService } from '../../services';
import { IUpdateUserReqDto } from './i-update-user.req.dto';

@Controller()
export class UpdateUserController {
  constructor(
    @Inject(UpdateUserService)
    private readonly updateUserService: IUpdateUserService,
  ) {}

  @TypedRoute.Patch()
  @HttpCode(HttpStatus.NO_CONTENT)
  public async execute(
    @CurrentUser() user: JwtPayload,
    @TypedBody() body: IUpdateUserReqDto,
  ): Promise<void> {
    await this.updateUserService.execute(
      new UpdateUserServiceInput(
        user.userId,
        body.nickname,
        body.birthDate,
        body.countryCode,
      ),
    );
  }
}