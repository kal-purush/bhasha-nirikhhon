import { Category } from '@application/entities/category.entity';
import { PlanningByCategory } from '@application/entities/planningByCategory.entity';
import {
  Category as RawCategory,
  PlanningByCategory as RawPlanningByCategory,
} from '@prisma/client';

export class PrismaPlanningByCategoryMapper {
  static toPrisma(planningByCategory: PlanningByCategory) {
    return {
      id: planningByCategory.id,
      goal: planningByCategory.goal,
      createdAt: planningByCategory.createdAt,
      categoryId: planningByCategory.category.id,
    };
  }

  static toDomain(raw: RawPlanningByCategory, subRaw: RawCategory) {
    return new PlanningByCategory(
      {
        goal: raw.goal,
        category: new Category(
          {
            name: subRaw.name,
            type: subRaw.type,
            createdAt: subRaw.createdAt,
          },
          subRaw.id,
        ),
        createdAt: raw.createdAt,
      },
      raw.id,
    );
  }
}