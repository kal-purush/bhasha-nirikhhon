import { HttpException, Injectable } from '@nestjs/common';
import { PlanningRepository } from '@application/repositories/planning-repository';

interface DeletePlanningByIdRequest {
  planning_uuid: string;
}

@Injectable()
export class DeletePlanningById {
  constructor(private planningRepository: PlanningRepository) {}

  async execute({ planning_uuid }: DeletePlanningByIdRequest): Promise<void> {
    const planning = await this.planningRepository.findPlanningById(
      planning_uuid,
    );

    if (!planning) {
      throw new HttpException(
        'Nenhum planejamento foi encontrado com este uuid!',
        404,
      );
    }

    await this.planningRepository.delete(planning_uuid);
  }
}