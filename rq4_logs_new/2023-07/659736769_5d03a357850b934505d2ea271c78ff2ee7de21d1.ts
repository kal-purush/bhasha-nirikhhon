import { Category } from '@application/entities/category.entity';
import { Expense } from '@application/entities/expense.entity';
import { Expense as RawExpense, Category as RawCategory } from '@prisma/client';

export class PrismaExpenseMapper {
  static toPrisma(expense: Expense) {
    return {
      id: expense.id,
      date: expense.date,
      value: expense.value,
      createdAt: expense.createdAt,
      isPay: expense.isPay,
      categoryId: expense.category.id,
      description: expense.description,
    };
  }

  static toDomain(raw: RawExpense, subRaw: RawCategory) {
    return new Expense(
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
        isPay: raw.isPay,
        description: raw.description,
      },
      raw.id,
    );
  }
}