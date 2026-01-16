import { type INestBaseResponse, Nest } from '../../api';

import { isObject } from '@repo/services/object/object';

import { QueryParameters } from '../../shared';
import { Paginate } from '../../paginate';

import ExpenseCategory from './expenseCategory';

import { ExpenseCategoryEntity } from './interface';

export class ExpenseCategoryService {
  constructor(private nest: Nest) {}

  public async create(name: string): Promise<ExpenseCategory> {
    return this.nest.finance.expense.category
      .create({ name })
      .then((response) => new ExpenseCategory(response));
  }

  public async update(param: string, name: string): Promise<ExpenseCategory> {
    return this.nest.finance.expense.category
      .update(param, { name })
      .then((response) => new ExpenseCategory(response));
  }

  public async getAll(
    parameters: QueryParameters,
  ): Promise<Array<ExpenseCategory> | Paginate<ExpenseCategory>> {
    return await this.nest.finance.expense.category
      .getAll(parameters)
      .then((response) => {
        if (isObject(response)) {
          const responsePaginate = response as Paginate<ExpenseCategoryEntity>;
          return {
            ...responsePaginate,
            results: responsePaginate.results.map(
              (result) => new ExpenseCategory(result),
            ),
          };
        }
        const responseArray = response as Array<ExpenseCategoryEntity>;
        return responseArray.map((result) => new ExpenseCategory(result));
      });
  }

  public async get(param: string): Promise<ExpenseCategory> {
    return await this.nest.finance.expense.category
      .getOne(param)
      .then((response) => new ExpenseCategory(response));
  }

  public async remove(param: string): Promise<INestBaseResponse> {
    return await this.nest.finance.expense.category.type.delete(param);
  }
}