import { Injectable } from '@nestjs/common';
import { PrismaService } from '@infra/database/prisma.service';
import { Category } from '@application/entities/category.entity';
import { PrismaCategoryMapper } from '../mappers/prisma-category-mapper';
import { CategoryRepository } from '@application/repositories/category-repository';

@Injectable()
export class PrismaCategoryRepository implements CategoryRepository {
  constructor(private prisma: PrismaService) {}

  async create(user_uuid: string, category: Category): Promise<void> {
    const categoryRaw = PrismaCategoryMapper.toPrisma(category);

    await this.prisma.category.create({
      data: { ...categoryRaw, userId: user_uuid },
    });
  }

  async update(category: Category): Promise<void> {
    const categoryRaw = PrismaCategoryMapper.toPrisma(category);

    await this.prisma.category.update({
      where: {
        id: categoryRaw.id,
      },
      data: categoryRaw,
    });
  }

  async findCategoryById(category_uuid: string): Promise<Category | null> {
    const category = await this.prisma.category.findUnique({
      where: {
        id: category_uuid,
      },
    });

    if (!category) {
      return null;
    }

    return PrismaCategoryMapper.toDomain(category);
  }

  async getAllCategories(user_uuid: string): Promise<Category[]> {
    console.log(user_uuid);
    const categories = await this.prisma.category.findMany({
      where: {
        userId: user_uuid,
      },
    });

    const adjustedCategories = categories.map((category) =>
      PrismaCategoryMapper.toDomain(category),
    );

    return adjustedCategories;
  }
}