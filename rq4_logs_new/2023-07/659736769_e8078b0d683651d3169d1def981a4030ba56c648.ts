import { Category } from '@application/entities/category.entity';
import { CategoryRepository } from '@application/repositories/category-repository';
import { HttpException, Injectable } from '@nestjs/common';

interface FindCategoryByIdRequest {
  category_uuid: string;
}

interface FindCategoryByIdResponse {
  category: Category;
}

@Injectable()
export class FindCategoryById {
  constructor(private categoryRepository: CategoryRepository) {}

  async execute({
    category_uuid,
  }: FindCategoryByIdRequest): Promise<FindCategoryByIdResponse> {
    const category = await this.categoryRepository.findCategoryById(
      category_uuid,
    );

    if (!category) {
      throw new HttpException('A categoria não foi encontrada!', 404);
    }

    return { category };
  }
}