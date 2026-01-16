import { Category } from '@application/entities/category.entity';
import { Income } from '@application/entities/income.entity';
import { Income as RawIncome, Category as RawCategory } from '@prisma/client';

export class PrismaIncomeMapper {
  static toPrisma(income: Income) {
    return {
      id: income.id,
      date: income.date,
      value: income.value,
      createdAt: income.createdAt,
      isReceived: income.isReceived,
      categoryId: income.category.id,
      description: income.description,
    };
  }

  static toDomain(raw: RawIncome, subRaw: RawCategory) {
    return new Income(
      {
        date: raw.date,
        value: raw.value,
        category: new Category(
          {
            name: subRaw.name,
            type: subRaw.type,
            createdAt: subRaw.createdAt,
          },
          subRaw.id,
        ),
        createdAt: raw.createdAt,
        isReceived: raw.isReceived,
        description: raw.description,
      },
      raw.id,
    );
  }
}