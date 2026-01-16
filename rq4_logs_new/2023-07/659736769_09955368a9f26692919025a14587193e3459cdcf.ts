import { PlanningByCategoryRepository } from '@application/repositories/planning-by-category-repository';
import { HttpException, Injectable } from '@nestjs/common';

interface DeletePlanningByCategoryByIdRequest {
  planningByCategory_uuid: string;
}

@Injectable()
export class DeletePlanningByCategoryById {
  constructor(
    private planningByCategoryRepository: PlanningByCategoryRepository,
  ) {}

  async execute({
    planningByCategory_uuid,
  }: DeletePlanningByCategoryByIdRequest): Promise<void> {
    const planningByCategory =
      await this.planningByCategoryRepository.findPlanningByCategoryById(
        planningByCategory_uuid,
      );

    if (!planningByCategory) {
      throw new HttpException(
        'Nenhum planejamento de categoria foi encontrado com este uuid!',
        404,
      );
    }

    await this.planningByCategoryRepository.deletePlanningByCategoryById(
      planningByCategory_uuid,
    );
  }
}