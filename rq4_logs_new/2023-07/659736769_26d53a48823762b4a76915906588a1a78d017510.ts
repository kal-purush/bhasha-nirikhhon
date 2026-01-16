import { PlanningByCategory } from '@application/entities/planningByCategory.entity';

export class PlanningByCategoryViewModel {
  static toHTTP(planningByCategory: PlanningByCategory) {
    return {
      id: planningByCategory.id,
      goal: planningByCategory.goal,
      category: {
        id: planningByCategory.category.id,
        name: planningByCategory.category.name,
        type: planningByCategory.category.type,
        createdAt: planningByCategory.category.createdAt,
      },
      createdAt: planningByCategory.createdAt,
    };
  }
}