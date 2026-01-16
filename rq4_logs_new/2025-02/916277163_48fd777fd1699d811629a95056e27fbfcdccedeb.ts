import { Router } from 'express';

import { createByName, findAll, findOne } from '../../shared';

import { LIST_EXPENSE_CATEGORY_TYPE_FIXTURE } from './fixtures';

const expenseCategoryTypeRouter = Router();

expenseCategoryTypeRouter.get(
  '/finance/expense/category/list/type',
  (req, res) => findAll(req, res, LIST_EXPENSE_CATEGORY_TYPE_FIXTURE),
);

expenseCategoryTypeRouter.get(
  '/finance/expense/category/:param/type',
  (req, res) =>
    findOne(
      req,
      res,
      LIST_EXPENSE_CATEGORY_TYPE_FIXTURE,
      'expense_category_types',
    ),
);

expenseCategoryTypeRouter.post('/finance/expense/category/type', (req, res) =>
  createByName(req, res, LIST_EXPENSE_CATEGORY_TYPE_FIXTURE),
);

export default expenseCategoryTypeRouter;