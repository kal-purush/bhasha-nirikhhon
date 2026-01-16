import type express from 'express'

import type { ExpenseService } from '../services/expenseService'
import type { Expense, Group } from '../type'
import { calculateSettlements } from '../utils/settlements'
import { ExpenseController } from './expenseController'

const setup = () => {
  const group: Group = {
    name: 'テストグループ1',
    members: ['一郎', '二郎', '三郎'],
  }

  const expense: Expense = {
    groupName: 'テストグループ1',
    expenseName: '支出1',
    amount: 2000,
    payer: '一郎',
  }

  const error = new Error('テストエラー')

  return { group, expense, error }
}

describe('ExpenseController', () => {
  let mockExpenseService: Partial<ExpenseService>
  let req: Partial<express.Request>
  let res: Partial<express.Response>
  let next: jest.Mock
  let expenseController: ExpenseController

  beforeEach(() => {
    mockExpenseService = {
      getSettlements: jest.fn(),
      addExpense: jest.fn(),
    }

    expenseController = new ExpenseController(
      mockExpenseService as ExpenseService,
    )

    req = {}
    res = {
      status: jest.fn().mockReturnThis(),
      json: jest.fn(),
      send: jest.fn(),
    }
    next = jest.fn()
  })

  describe('getSettlements', () => {
    it('精算リストが返却される', () => {
      const { group, expense } = setup()

      req.params = { groupName: group.name }
      const settlements = calculateSettlements([expense], group.members)
      ;(mockExpenseService.getSettlements as jest.Mock).mockReturnValueOnce(
        settlements,
      )
      expenseController.getSettlements(
        req as express.Request,
        res as express.Response,
        next as express.NextFunction,
      )
      expect(res.status).toHaveBeenCalledWith(200)
      expect(res.json).toHaveBeenCalledWith(settlements)
      expect(mockExpenseService.getSettlements).toHaveBeenCalledWith(group.name)
    })

    it('expenseServiceでエラーが発生した場合、nextFunctionが実行される', () => {
      const { group, error } = setup()
      req.params = { groupName: group.name }
      ;(mockExpenseService.getSettlements as jest.Mock).mockImplementationOnce(
        () => {
          throw error
        },
      )
      expenseController.getSettlements(
        req as express.Request,
        res as express.Response,
        next as express.NextFunction,
      )
      expect(next).toHaveBeenCalledWith(error)
    })
  })

  describe('addExpense', () => {
    it('expenseを保存できる', () => {
      const { expense } = setup()

      req.body = {
        groupName: expense.groupName,
        expenseName: expense.expenseName,
        payer: expense.payer,
        amount: expense.amount,
      }
      expenseController.addExpense(
        req as express.Request,
        res as express.Response,
        next as express.NextFunction,
      )
      expect(res.status).toHaveBeenCalledWith(200)
      expect(res.send).toHaveBeenCalledWith('支出が登録されました')
      expect(mockExpenseService.addExpense).toHaveBeenCalledWith(expense)
    })

    it('expenseServiceでエラーが発生した場合、nextFunctionが実行される', () => {
      const { expense, error } = setup()

      req.body = {
        groupName: expense.groupName,
        expenseName: expense.expenseName,
        payer: expense.payer,
        amount: expense.amount,
      }
      ;(mockExpenseService.addExpense as jest.Mock).mockImplementationOnce(
        () => {
          throw error
        },
      )
      expenseController.addExpense(
        req as express.Request,
        res as express.Response,
        next as express.NextFunction,
      )
      expect(next).toHaveBeenCalledWith(error)
    })

    it('バリデーションエラー: グループ名が必須', () => {
      const { expense } = setup()

      req.body = {
        groupName: '',
        expenseName: expense.expenseName,
        payer: expense.payer,
        amount: expense.amount,
      }
      expenseController.addExpense(
        req as express.Request,
        res as express.Response,
        next as express.NextFunction,
      )
      expect(res.status).toHaveBeenCalledWith(400)
      expect(res.send).toHaveBeenCalledWith(['グループ名は必須です'])
    })

    it('バリデーションエラー: 支出名が必須', () => {
      const { expense } = setup()

      req.body = {
        groupName: expense.groupName,
        expenseName: '',
        payer: expense.payer,
        amount: expense.amount,
      }
      expenseController.addExpense(
        req as express.Request,
        res as express.Response,
        next as express.NextFunction,
      )
      expect(res.status).toHaveBeenCalledWith(400)
      expect(res.send).toHaveBeenCalledWith(['支出名は必須です'])
    })

    it('バリデーションエラー: 支払うメンバーが必須', () => {
      const { expense } = setup()

      req.body = {
        groupName: expense.groupName,
        expenseName: expense.expenseName,
        payer: '',
        amount: expense.amount,
      }
      expenseController.addExpense(
        req as express.Request,
        res as express.Response,
        next as express.NextFunction,
      )
      expect(res.status).toHaveBeenCalledWith(400)
      expect(res.send).toHaveBeenCalledWith(['支払うメンバーは必須です'])
    })

    it('バリデーションエラー: 金額は1円以上', () => {
      const { expense } = setup()

      req.body = {
        groupName: expense.groupName,
        expenseName: expense.expenseName,
        payer: expense.payer,
        amount: 0,
      }
      expenseController.addExpense(
        req as express.Request,
        res as express.Response,
        next as express.NextFunction,
      )
      expect(res.status).toHaveBeenCalledWith(400)
      expect(res.send).toHaveBeenCalledWith(['金額は1円以上の整数です'])
    })
  })
})