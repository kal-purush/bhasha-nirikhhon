import {
  Res,
  Body,
  Patch,
  Param,
  Delete,
  UseGuards,
  Controller,
} from '@nestjs/common';
import { Response } from 'express';
import { JwtGuard } from '@application/use-cases/auth/guards/jwt-auth.guard';
import { UpdatePlanningByCategoryBody } from '../dtos/planning-by-category/update-planning-by-category-body';
import { UpdatePlanningByCategoryById } from '@application/use-cases/planning-by-category/update-planning-by-category-by-id';
import { DeletePlanningByCategoryById } from '@application/use-cases/planning-by-category/delete-planning-by-category-by-id';

@UseGuards(JwtGuard)
@Controller('plannings-by-category')
export class PlanningByCategoryController {
  constructor(
    private updatePlanningByCategoryById: UpdatePlanningByCategoryById,
    private deletePlanningByCategoryById: DeletePlanningByCategoryById,
  ) {}

  @Patch('update-planning-by-category-by/:id')
  async update(
    @Param() param,
    @Body() bodyRequest: UpdatePlanningByCategoryBody,
    @Res()
    response: Response,
  ) {
    const planningByCategory_uuid = param.id;

    await this.updatePlanningByCategoryById.execute({
      planningByCategory_uuid,
      ...bodyRequest,
    });

    response.json({
      status: 201,
      message: 'Planejamento da categoria atualizado com sucesso!',
    });
  }

  @Delete('delete-planning-by-category-by/:id')
  async delete(
    @Param() param,
    @Res()
    response: Response,
  ) {
    const planningByCategory_uuid = param.id;

    await this.deletePlanningByCategoryById.execute({
      planningByCategory_uuid,
    });

    response.json({
      status: 204,
      message: 'Planejamento da categoria deletado com sucesso!',
    });
  }
}