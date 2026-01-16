import { Module } from '@nestjs/common';
import { DatabaseModule } from '@infra/database/prisma/database.module';
import { CategoryController } from '../controllers/category.controller';
import { CreateCategory } from '@application/use-cases/category/create-category';
import { UpdateCategoryById } from '@application/use-cases/category/update-category-by-id';
import { GetAllCategories } from '@application/use-cases/category/get-all-categories';

@Module({
  imports: [DatabaseModule],
  controllers: [CategoryController],
  providers: [CreateCategory, UpdateCategoryById, GetAllCategories],
})
export class CategoryModule {}