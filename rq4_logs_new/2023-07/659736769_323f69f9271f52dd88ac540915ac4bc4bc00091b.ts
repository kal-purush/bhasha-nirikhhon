import { HttpException, Injectable } from '@nestjs/common';
import { Expense } from '@application/entities/expense.entity';
import { ExpenseRepository } from '@application/repositories/expense-repository';

interface FindExpenseByIdRequest {
  expense_uuid: string;
}

interface FindExpenseByIdResponse {
  expense: Expense;
}

@Injectable()
export class FindExpenseById {
  constructor(private expenseRepository: ExpenseRepository) {}

  async execute({
    expense_uuid,
  }: FindExpenseByIdRequest): Promise<FindExpenseByIdResponse> {
    const expense = await this.expenseRepository.findExpenseById(expense_uuid);

    if (!expense) {
      throw new HttpException('A despesa não foi encontrada!', 404);
    }

    return { expense };
  }
}