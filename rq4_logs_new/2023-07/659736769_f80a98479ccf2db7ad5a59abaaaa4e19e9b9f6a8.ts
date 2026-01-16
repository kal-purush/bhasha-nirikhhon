import { parse } from 'date-fns';
import { HttpException, Injectable } from '@nestjs/common';
import { Category } from '@application/entities/category.entity';
import { CategoryRepository } from '@application/repositories/category-repository';
import { ExpenseRepository } from '@application/repositories/expense-repository';
import { Expense } from '@application/entities/expense.entity';

interface UpdateExpenseByIdRequest {
  expense_uuid: string;
  category_uuid: string;
  description: string;
  date: string;
  value: number;
  isPay: boolean;
}

@Injectable()
export class UpdateExpenseById {
  constructor(
    private expenseRepository: ExpenseRepository,
    private categoryRepository: CategoryRepository,
  ) {}

  async execute({
    expense_uuid,
    category_uuid,
    description,
    date,
    value,
    isPay,
  }: UpdateExpenseByIdRequest) {
    let category: Category;

    const currentExpense = await this.expenseRepository.findExpenseById(
      expense_uuid,
    );

    if (!currentExpense) {
      throw new HttpException('A despesa não foi encontrada!', 404);
    }

    const checkIfCategoryIsSame = category_uuid
      ? category_uuid != currentExpense.category.id
      : false;

    if (checkIfCategoryIsSame) {
      category = await this.categoryRepository.findCategoryById(category_uuid);

      if (!category) {
        throw new HttpException('A categoria não foi encontrada!', 404);
      }
    }

    const expense = new Expense(
      {
        createdAt: currentExpense.createdAt,
        value: value ?? currentExpense.value,
        isPay: isPay ?? currentExpense.isPay,
        description: description ?? currentExpense.description,
        category: checkIfCategoryIsSame ? category : currentExpense.category,
        date: date
          ? parse(date, 'dd/MM/yyyy', new Date())
          : currentExpense.date,
      },
      currentExpense.id,
    );

    await this.expenseRepository.update(expense);
  }
}