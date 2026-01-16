import { Expense } from '@application/entities/expense.entity';
import { format } from 'date-fns';

export class ExpenseViewModel {
  static toHTTP(expense: Expense) {
    return {
      id: expense.id,
      value: expense.value,
      date: format(expense.date, 'dd/MM/yyy'),
      isPay: expense.isPay,
      description: expense.description,
      category: {
        id: expense.category.id,
        type: expense.category.type,
        name: expense.category.name,
        createdAt: expense.category.createdAt,
      },
      createdAt: expense.createdAt,
    };
  }
}