import { Request, Response } from 'express';

import { EExpenseType, EMonth } from '@repo/business/finance/enum';
import type { ExpenseEntity } from '@repo/business/finance/expense/interface';
import ExpenseBusiness from '@repo/business/finance/expense/expenseBusiness';

import { buildResponse } from '../../shared';
import { USER_FIXTURE } from '../../auth';

import { findEntityByName, validateEntityType } from '../shared';

import { EXPENSE_FINANCE_ENTITY } from './fixtures';

export function create(req: Request, res: Response) {
  const {
    year = new Date().getFullYear(),
    type = EExpenseType.VARIABLE,
    paid = false,
    value = 0,
    month = EMonth.JANUARY,
    group,
    supplier,
    category,
    description,
    instalment_number = 1,
  } = req.body;

  const financeEntity = EXPENSE_FINANCE_ENTITY;

  const groupEntity = findEntityByName(group, financeEntity?.group?.list);

  if (!groupEntity) {
    return buildResponse(
      res,
      validateEntityType(false, financeEntity?.group?.label),
    );
  }
  const supplierEntity = findEntityByName(
    supplier,
    financeEntity?.supplier?.list,
  );
  if (!supplierEntity) {
    return buildResponse(
      res,
      validateEntityType(false, financeEntity?.supplier?.label),
    );
  }
  const categoryEntity = findEntityByName(
    category,
    financeEntity?.category?.list,
  );
  if (!categoryEntity) {
    return buildResponse(
      res,
      validateEntityType(false, financeEntity?.category?.label),
    );
  }

  const expense = {
    id: '1781d932-aaf8-4c8d-be3b-db23ffa602c7',
    year,
    user: USER_FIXTURE,
    type,
    paid,
    total: 0,
    month,
    value,
    group: groupEntity as ExpenseEntity['group'],
    active: true,
    supplier: supplierEntity as ExpenseEntity['supplier'],
    category: categoryEntity as ExpenseEntity['category'],
    total_paid: 0,
    january: 0,
    february: 0,
    march: 0,
    april: 0,
    may: 0,
    june: 0,
    july: 0,
    august: 0,
    september: 0,
    october: 0,
    november: 0,
    december: 0,
    january_paid: false,
    february_paid: false,
    march_paid: false,
    april_paid: false,
    may_paid: false,
    june_paid: false,
    july_paid: false,
    august_paid: false,
    september_paid: false,
    october_paid: false,
    november_paid: false,
    december_paid: false,
    created_at: new Date(),
    updated_at: new Date(),
    deleted_at: undefined,
    description,
    instalment_number,
  };

  const expenseBusiness = new ExpenseBusiness();
  const response = expenseBusiness.initializeExpense(expense);
  return buildResponse(res, {
    statusCode: 200,
    response,
  });
}