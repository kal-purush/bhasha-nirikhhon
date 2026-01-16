import { Injectable } from '@nestjs/common';
import { PrismaService } from '@infra/database/prisma.service';
import { Planning } from '@application/entities/planning.entity';
import { PrismaPlanningMapper } from '../mappers/prisma-planning-mapper';
import { PlanningRepository } from '@application/repositories/planning-repository';
import { PrismaPlanningByCategoryMapper } from '../mappers/prisma-planning-by-category-mapper';

@Injectable()
export class PrismaPlanningRepository implements PlanningRepository {
  constructor(private prisma: PrismaService) {}

  async create(user_uuid: string, planning: Planning): Promise<void> {
    const rawPlanning = PrismaPlanningMapper.toPrisma(planning);
    console.log(planning.planningsByCategory);
    const rawPlanningsByCategory = planning.planningsByCategory.map(
      (planningByCategory) =>
        PrismaPlanningByCategoryMapper.toPrisma(planningByCategory),
    );
    console.log(rawPlanningsByCategory);
    await this.prisma.planning.create({
      data: {
        ...rawPlanning,
        userId: user_uuid,
        PlanningByCategory: {
          createMany: {
            data: rawPlanningsByCategory,
          },
        },
      },
    });
  }
}