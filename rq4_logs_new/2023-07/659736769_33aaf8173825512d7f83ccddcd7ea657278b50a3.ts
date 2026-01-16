import { Injectable } from '@nestjs/common';
import { PrismaService } from '@infra/database/prisma.service';
import { Expense } from '@application/entities/expense.entity';
import { PrismaExpenseMapper } from '../mappers/prisma-expense-mapper';
import { ExpenseRepository } from '@application/repositories/expense-repository';

@Injectable()
export class PrismaExpenseRepository implements ExpenseRepository {
  constructor(private prisma: PrismaService) {}

  async create(user_uuid: string, expense: Expense): Promise<void> {
    const rawExpense = PrismaExpenseMapper.toPrisma(expense);

    await this.prisma.expense.create({
      data: { ...rawExpense, userId: user_uuid },
    });
  }

  async update(expense: Expense): Promise<void> {
    const rawExpense = PrismaExpenseMapper.toPrisma(expense);

    await this.prisma.expense.update({
      where: {
        id: rawExpense.id,
      },
      data: rawExpense,
    });
  }

  async findExpenseById(expense_uuid: string): Promise<Expense | null> {
    const expense = await this.prisma.expense.findUnique({
      where: {
        id: expense_uuid,
      },
      include: { category: true },
    });

    if (!expense) {
      return null;
    }

    return PrismaExpenseMapper.toDomain(expense, expense.category);
  }

  async getExpensesOfMonth(
    user_uuid: string,
    initialDate: Date,
    finalDate: Date,
  ): Promise<Expense[]> {
    const expenses = await this.prisma.expense.findMany({
      where: {
        userId: user_uuid,
        date: {
          lte: finalDate,
          gte: initialDate,
        },
      },
      include: { category: true },
    });

    const adjustedExpenses = expenses.map((expense) =>
      PrismaExpenseMapper.toDomain(expense, expense.category),
    );

    return adjustedExpenses;
  }
}