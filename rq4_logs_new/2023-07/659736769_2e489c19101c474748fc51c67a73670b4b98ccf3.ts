import { parse } from 'date-fns';
import { HttpException, Injectable } from '@nestjs/common';
import { PlanningRepository } from '@application/repositories/planning-repository';
import { Planning } from '@application/entities/planning.entity';
import { PlanningByCategory } from '@application/entities/planningByCategory.entity';
import { CategoryRepository } from '@application/repositories/category-repository';

interface CreatePlanningRequest {
  user_uuid: string;
  month: string;
  goal: number;
  planningsByCategory: [
    {
      goal: number;
      category_uuid: string;
    },
  ];
}

@Injectable()
export class CreatePlanning {
  constructor(
    private planningRepository: PlanningRepository,
    private categoryRepository: CategoryRepository,
  ) {}

  async execute({
    user_uuid,
    month,
    goal,
    planningsByCategory,
  }: CreatePlanningRequest): Promise<void> {
    //const parsedDate = parse(`${month}`, 'MM', new Date());

    const planning = new Planning({ month, goal });

    planningsByCategory.forEach(async (planningByCategory) => {
      const category = await this.categoryRepository.findCategoryById(
        planningByCategory.category_uuid,
      );

      if (!category) {
        throw new HttpException('A categoria não foi encontrada!', 404);
      }

      const currentPlanningByCategory = new PlanningByCategory({
        goal: planningByCategory.goal,
        category: category,
      });

      planning.planningsByCategory = currentPlanningByCategory;
      console.log(planning.planningsByCategory);
    });

    console.log(planning.planningsByCategory);

    await this.planningRepository.create(user_uuid, planning);
  }
}