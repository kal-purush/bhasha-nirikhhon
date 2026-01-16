import { HttpException, Injectable } from '@nestjs/common';
import { Category } from '@application/entities/category.entity';
import { CategoryRepository } from '@application/repositories/category-repository';

interface UpdateCategoryByIdRequest {
  category_uuid: string;
  name?: string;
  type?: string;
}

@Injectable()
export class UpdateCategoryById {
  constructor(private categoryRepository: CategoryRepository) {}

  async execute({
    category_uuid,
    name,
    type,
  }: UpdateCategoryByIdRequest): Promise<void> {
    const currentCategory = await this.categoryRepository.findCategoryById(
      category_uuid,
    );

    if (!currentCategory) {
      throw new HttpException('A categoria não foi encontrada!', 404);
    }

    const updatedCategory = new Category(
      {
        name: name ?? currentCategory.name,
        type: type ?? currentCategory.type,
        createdAt: currentCategory.createdAt,
      },
      currentCategory.id,
    );

    await this.categoryRepository.update(updatedCategory);
  }
}