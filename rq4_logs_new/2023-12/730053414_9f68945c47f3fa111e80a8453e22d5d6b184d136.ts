import type { ExpenseRepository } from '../repositories/expenseRepository'
import type { Expense, Group } from '../type'
import { calculateSettlements } from '../utils/settlements'
import { ExpenseService } from './expenseService'
import type { GroupService } from './groupService'

describe('ExpenseService', () => {
  // サービスクラスなどはリポジトリクラスなど依存しているクラスのメソッドや関数を使用しているため、それらの関数をモック化する必要がある
  let mockExpenseRepository: Partial<ExpenseRepository>
  let mockGroupService: Partial<GroupService>
  let expenseService: ExpenseService

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

  beforeEach(() => {
    // Partial型にすることでgetGroupByNameのみモック化することができ、その他の使用していないもののモック化をしなくてもよくなる
    mockGroupService = {
      getGroupByName: jest.fn(),
    }
    mockExpenseRepository = {
      loadExpenses: jest.fn(),
      saveExpense: jest.fn(),
    }

    expenseService = new ExpenseService(
      mockExpenseRepository as ExpenseRepository,
      mockGroupService as GroupService,
    )
  })

  describe('getSettlements', () => {
    it('groupが存在する場合、精算リストを返却する', () => {
      ;(mockGroupService.getGroupByName as jest.Mock).mockReturnValueOnce(group)
      ;(mockExpenseRepository.loadExpenses as jest.Mock).mockReturnValueOnce([
        expense,
      ])
      const settlements = calculateSettlements([expense], group.members)
      const result = expenseService.getSettlements(group.name)
      expect(mockExpenseRepository.loadExpenses).toHaveBeenCalledTimes(1)
      expect(result).toEqual(settlements)
    })

    it('groupが存在しない場合、エラーとなる', () => {
      ;(mockGroupService.getGroupByName as jest.Mock).mockReturnValueOnce(
        undefined,
      )
      expect(() => expenseService.getSettlements(group.name)).toThrowError(
        `グループ： ${group.name} が存在しません`,
      )
    })
  })

  describe('addExpense', () => {
    it('groupが存在する場合、expenseを保存できる', () => {
      ;(mockGroupService.getGroupByName as jest.Mock).mockReturnValueOnce(group)
      expenseService.addExpense(expense)
      expect(mockExpenseRepository.saveExpense).toHaveBeenCalledWith(expense)
    })

    it('groupが存在しない場合、エラーとなる', () => {
      ;(mockGroupService.getGroupByName as jest.Mock).mockReturnValueOnce(
        undefined,
      )
      expect(() => expenseService.addExpense(expense)).toThrowError(
        `グループ： ${group.name} が存在しません`,
      )
    })

    it('group内にpayer指定されたメンバーがいない場合、エラーとなる', () => {
      ;(mockGroupService.getGroupByName as jest.Mock).mockReturnValueOnce(group)
      const nonMemberExpense: Expense = { ...expense, payer: '四郎' }
      expect(() => expenseService.addExpense(nonMemberExpense)).toThrowError(
        '支払い者がメンバーの中にいません',
      )
    })
  })
})