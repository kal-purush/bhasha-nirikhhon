import { Expense } from '@application/entities/expense.entity';
import { CategoryRepository } from '@application/repositories/category-repository';
import { ExpenseRepository } from '@application/repositories/expense-repository';
import { HttpException, Injectable } from '@nestjs/common';
import { parse } from 'date-fns';

interface CreateExpenseRequest {
  user_uuid: string;
  category_uuid: string;
  date: string;
  value: number;
  isPay: boolean;
  description: string;
}

@Injectable()
export class CreateExpense {
  constructor(
    private expenseRepository: ExpenseRepository,
    private categoryRepository: CategoryRepository,
  ) {}

  async execute({
    user_uuid,
    category_uuid,
    date,
    value,
    isPay,
    description,
  }: CreateExpenseRequest): Promise<void> {
    const category = await this.categoryRepository.findCategoryById(
      category_uuid,
    );

    if (!category) {
      throw new HttpException('A categoria não foi encontrada!', 404);
    }

    const parsedDate = parse(date, 'dd/MM/yyyy', new Date());

    const expense = new Expense({
      date: parsedDate,
      value,
      isPay,
      description,
      category,
    });

    await this.expenseRepository.create(user_uuid, expense);
  }
}