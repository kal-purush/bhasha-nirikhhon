import { TypedParam, TypedRoute } from '@nestia/core';
import { Controller, HttpCode, HttpStatus, Inject } from '@nestjs/common';

import { JwtPayload } from '../../../../core/auth';
import {
  DeleteSubjectByIdServiceInput,
  IDeleteSubjectByIdService,
} from '../../../../core/subject';
import { CurrentUser } from '../../../shared/decorators';
import { DeleteSubjectByIdService } from '../../services';

@Controller()
export class DeleteSubjectController {
  constructor(
    @Inject(DeleteSubjectByIdService)
    private readonly deleteSubjectByIdService: IDeleteSubjectByIdService,
  ) {}

  @TypedRoute.Delete(':subjectId')
  @HttpCode(HttpStatus.NO_CONTENT)
  public async execute(
    @CurrentUser() user: JwtPayload,
    @TypedParam('subjectId') subjectId: string,
  ): Promise<void> {
    await this.deleteSubjectByIdService.execute(
      new DeleteSubjectByIdServiceInput(subjectId, user.userId),
    );
  }
}