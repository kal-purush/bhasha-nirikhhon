import { Injectable } from '@nestjs/common';
import { PrismaService } from '@infra/database/prisma.service';
import { PlanningByCategory } from '@application/entities/planningByCategory.entity';
import { PrismaPlanningByCategoryMapper } from '../mappers/prisma-planning-by-category-mapper';
import { PlanningByCategoryRepository } from '@application/repositories/planning-by-category-repository';

@Injectable()
export class PrismaPlanningByCategoryRepository
  implements PlanningByCategoryRepository
{
  constructor(private prisma: PrismaService) {}

  async update(planningByCategory: PlanningByCategory): Promise<void> {
    const rawPlanningByCategory =
      PrismaPlanningByCategoryMapper.toPrisma(planningByCategory);

    await this.prisma.planningByCategory.update({
      where: {
        id: rawPlanningByCategory.id,
      },
      data: rawPlanningByCategory,
    });
  }

  async findPlanningByCategoryById(
    planningByCategory_uuid: string,
  ): Promise<PlanningByCategory | null> {
    const planningByCategory = await this.prisma.planningByCategory.findUnique({
      where: {
        id: planningByCategory_uuid,
      },
      include: {
        category: true,
      },
    });

    if (!planningByCategory) {
      return null;
    }

    return PrismaPlanningByCategoryMapper.toDomain(
      planningByCategory,
      planningByCategory.category,
    );
  }

  async deletePlanningByCategoryById(
    planningByCategory_uuid: string,
  ): Promise<void> {
    await this.prisma.planningByCategory.delete({
      where: {
        id: planningByCategory_uuid,
      },
    });
  }
}