import { Module } from '@nestjs/common';
import { DatabaseModule } from '@infra/database/prisma/database.module';
import { PlanningByCategoryController } from '../controllers/planningByCategory.controller';
import { UpdatePlanningByCategoryById } from '@application/use-cases/planning-by-category/update-planning-by-category-by-id';
import { DeletePlanningByCategoryById } from '@application/use-cases/planning-by-category/delete-planning-by-category-by-id';

@Module({
  imports: [DatabaseModule],
  controllers: [PlanningByCategoryController],
  providers: [UpdatePlanningByCategoryById, DeletePlanningByCategoryById],
})
export class PlanningByCategoryModule {}