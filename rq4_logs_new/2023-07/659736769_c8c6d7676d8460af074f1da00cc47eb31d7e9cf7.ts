import { Category } from '@application/entities/category.entity';
import { Planning } from '@application/entities/planning.entity';
import {
  Planning as RawPlanning,
  Category as RawCategory,
  PlanningByCategory as RawPlanningByCategory,
} from '@prisma/client';

export class PrismaPlanningMapper {
  static toPrisma(planning: Planning) {
    return {
      id: planning.id,
      goal: planning.goal,
      month: planning.month,
      createdAt: planning.createdAt,
    };
  }

  static toDomain(
    raw: RawPlanning,
    subRaw: RawCategory,
    thirdRaw: RawPlanningByCategory,
  ) {
    return new Planning(
      {
        goal: raw.goal,
        month: raw.month,
        createdAt: raw.createdAt,
      },
      raw.id,
    );
  }
}