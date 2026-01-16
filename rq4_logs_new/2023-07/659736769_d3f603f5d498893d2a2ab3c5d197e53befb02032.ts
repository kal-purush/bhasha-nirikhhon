import { HttpException, Injectable } from '@nestjs/common';
import { ExpenseRepository } from '@application/repositories/expense-repository';

interface DeleteExpenseByIdRequest {
  expense_uuid: string;
}

@Injectable()
export class DeleteExpenseById {
  constructor(private expenseRepository: ExpenseRepository) {}

  async execute({ expense_uuid }: DeleteExpenseByIdRequest): Promise<void> {
    const expense = await this.expenseRepository.findExpenseById(expense_uuid);

    if (!expense) {
      throw new HttpException(
        'Nenhuma despesa foi encontrada com este uuid!',
        404,
      );
    }

    await this.expenseRepository.deleteExpenseById(expense_uuid);
  }
}