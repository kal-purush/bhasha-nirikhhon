import { Category } from '@application/entities/category.entity';
import { CategoryRepository } from '@application/repositories/category-repository';
import { Injectable } from '@nestjs/common';

interface GetAllCategoriesRequest {
  user_uuid: string;
}

interface GetAllCategoriesResponse {
  allCategories: Category[];
  incomeCategories: Category[];
  expenseCategories: Category[];
}

@Injectable()
export class GetAllCategories {
  constructor(private categoryRepository: CategoryRepository) {}

  async execute({
    user_uuid,
  }: GetAllCategoriesRequest): Promise<GetAllCategoriesResponse> {
    const allCategories = await this.categoryRepository.getAllCategories(
      user_uuid,
    );

    const incomeCategories = allCategories.filter(
      (category) => category.type === 'income',
    );

    const expenseCategories = allCategories.filter(
      (category) => category.type === 'expense',
    );

    return {
      allCategories,
      incomeCategories,
      expenseCategories,
    };
  }
}