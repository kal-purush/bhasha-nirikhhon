import { TypedBody, TypedRoute } from '@nestia/core';
import { Controller, Inject } from '@nestjs/common';

import { JwtPayload } from '../../../../core/auth';
import {
  CreateSubjectServiceInput,
  ICreateSubjectService,
} from '../../../../core/subject/i-services';
import { CurrentUser } from '../../../shared/decorators';
import { CreateSubjectService } from '../../services';
import { ICreateSubjectReqDto } from './i-create-subject.req.dto';
import { ICreateSubjectResDto } from './i-create-subject.res.dto';

@Controller()
export class CreateSubjectController {
  constructor(
    @Inject(CreateSubjectService)
    private readonly createSubjectService: ICreateSubjectService,
  ) {}

  @TypedRoute.Post()
  public async execute(
    @CurrentUser() user: JwtPayload,
    @TypedBody() body: ICreateSubjectReqDto,
  ): Promise<ICreateSubjectResDto> {
    const subject = await this.createSubjectService.execute(
      new CreateSubjectServiceInput(
        user.userId,
        body.title,
        body.color,
        body.order,
      ),
    );

    return { subjectId: subject.subjectId };
  }
}