import { HttpException, Injectable } from '@nestjs/common';
import { Planning } from '@application/entities/planning.entity';
import { PlanningRepository } from '@application/repositories/planning-repository';

interface FindPlanningByIdRequest {
  planning_uuid: string;
}

interface FindPlanningByIdResponse {
  planning: Planning;
}

@Injectable()
export class FindPlanningById {
  constructor(private planningRepository: PlanningRepository) {}

  async execute({
    planning_uuid,
  }: FindPlanningByIdRequest): Promise<FindPlanningByIdResponse> {
    const planning = await this.planningRepository.findPlanningById(
      planning_uuid,
    );

    if (!planning) {
      throw new HttpException('O planejamento não foi encontrado!', 404);
    }

    return { planning };
  }
}