import { HttpException, Injectable } from '@nestjs/common';
import { Category } from '@application/entities/category.entity';
import { CategoryRepository } from '@application/repositories/category-repository';
import { PlanningByCategory } from '@application/entities/planningByCategory.entity';
import { PlanningByCategoryRepository } from '@application/repositories/planning-by-category-repository';

interface UpdatePlanningByCategoryByIdRequest {
  planningByCategory_uuid: string;
  category_uuid: string;
  goal: number;
}

@Injectable()
export class UpdatePlanningByCategoryById {
  constructor(
    private planningByCategoryRepository: PlanningByCategoryRepository,
    private categoryRepository: CategoryRepository,
  ) {}

  async execute({
    planningByCategory_uuid,
    category_uuid,
    goal,
  }: UpdatePlanningByCategoryByIdRequest): Promise<void> {
    let category: Category;

    const currentPlanningByCategory =
      await this.planningByCategoryRepository.findPlanningByCategoryById(
        planningByCategory_uuid,
      );

    if (!currentPlanningByCategory) {
      throw new HttpException(
        'O planejamento da categoria não foi encontrado!',
        404,
      );
    }

    const checkIfCategoryIsSame = category_uuid
      ? category_uuid != currentPlanningByCategory.category.id
      : false;

    if (checkIfCategoryIsSame) {
      category = await this.categoryRepository.findCategoryById(category_uuid);

      if (!category) {
        throw new HttpException('A categoria não foi encontrada!', 404);
      }
    }

    const updatedPlanningByCategory = new PlanningByCategory(
      {
        goal: goal ?? currentPlanningByCategory.goal,
        category: checkIfCategoryIsSame
          ? category
          : currentPlanningByCategory.category,
        createdAt: currentPlanningByCategory.createdAt,
      },
      currentPlanningByCategory.id,
    );

    await this.planningByCategoryRepository.update(updatedPlanningByCategory);
  }
}