import { Category } from '@application/entities/category.entity';
import { Category as RawCategory } from '@prisma/client';

export class PrismaCategoryMapper {
  static toPrisma(category: Category) {
    return {
      id: category.id,
      name: category.name,
      type: category.type,
      createdAt: category.createdAt,
    };
  }

  static toDomain(raw: RawCategory): Category {
    return new Category(
      {
        name: raw.name,
        type: raw.type,
        createdAt: raw.createdAt,
      },
      raw.id,
    );
  }
}