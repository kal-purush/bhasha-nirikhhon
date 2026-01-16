import { Module } from '@nestjs/common';
import { ExpenseController } from '../controllers/expense.controller';
import { DatabaseModule } from '@infra/database/prisma/database.module';
import { CreateExpense } from '@application/use-cases/expense/create-expense';
import { FindExpenseById } from '@application/use-cases/expense/find-expense-by-id';
import { UpdateExpenseById } from '@application/use-cases/expense/update-expense-by-id';
import { GetExpensesOfMonth } from '@application/use-cases/expense/get-expenses-of-month';

@Module({
  imports: [DatabaseModule],
  controllers: [ExpenseController],
  providers: [
    CreateExpense,
    FindExpenseById,
    UpdateExpenseById,
    GetExpensesOfMonth,
  ],
})
export class ExpenseModule {}