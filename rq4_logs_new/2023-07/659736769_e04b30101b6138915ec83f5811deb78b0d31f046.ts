import { Planning } from '@application/entities/planning.entity';
import { PlanningByCategoryViewModel } from './planning-by-category-view-model';

export class PlanningViewModel {
  static toHTTP(planning: Planning) {
    return {
      id: planning.id,
      goal: planning.goal,
      month: planning.month,
      planningsByCategory: planning.planningsByCategory.map(
        (planningByCategory) =>
          PlanningByCategoryViewModel.toHTTP(planningByCategory),
      ),
      createdAt: planning.createdAt,
    };
  }
}