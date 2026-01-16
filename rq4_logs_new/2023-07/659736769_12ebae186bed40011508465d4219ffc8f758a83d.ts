import { Category } from '@application/entities/category.entity';
import { CategoryRepository } from '@application/repositories/category-repository';
import { Injectable } from '@nestjs/common';

interface CreateCategoryRequest {
  user_uuid: string;
  name: string;
  type: 'income' | 'expense';
}

@Injectable()
export class CreateCategory {
  constructor(private categoryRepository: CategoryRepository) {}

  async execute({ user_uuid, name, type }: CreateCategoryRequest) {
    const category = new Category({ name, type });

    await this.categoryRepository.create(user_uuid, category);
  }
}