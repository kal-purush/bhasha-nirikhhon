import { TypedBody, TypedRoute } from '@nestia/core';
import { Controller, HttpCode, HttpStatus } from '@nestjs/common';

import { JwtPayload } from '../../../../core/auth';
import {
  IUpdateSubjectService,
  UpdateSubjectServiceInput,
} from '../../../../core/subject/i-services/i-update-subject.service';
import { CurrentUser } from '../../../shared/decorators';
import { IUpdateSubjectReqDto } from './i-update-subject.req.dto';

@Controller()
export class UpdateSubjectController {
  constructor(private readonly updateSubjectService: IUpdateSubjectService) {}

  @TypedRoute.Patch(':subjectId')
  @HttpCode(HttpStatus.NO_CONTENT)
  public async execute(
    @CurrentUser() user: JwtPayload,
    @TypedBody() body: IUpdateSubjectReqDto,
  ): Promise<void> {
    await this.updateSubjectService.execute(
      new UpdateSubjectServiceInput(
        body.subjectId,
        user.userId,
        body.title,
        body.color,
        body.order,
      ),
    );
  }
}