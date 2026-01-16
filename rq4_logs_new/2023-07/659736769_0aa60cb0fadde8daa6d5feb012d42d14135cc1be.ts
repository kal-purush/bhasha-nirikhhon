import { Income } from '@application/entities/income.entity';
import { format } from 'date-fns';

export class IncomeViewModel {
  static toHTTP(income: Income) {
    return {
      id: income.id,
      value: income.value,
      date: format(income.date, 'dd/MM/yyy'),
      isReceived: income.isReceived,
      description: income.description,
      category: {
        id: income.category.id,
        type: income.category.type,
        name: income.category.name,
        createdAt: income.category.createdAt,
      },
      createdAt: income.createdAt,
    };
  }
}