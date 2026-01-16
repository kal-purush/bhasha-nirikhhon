import fs from 'fs'

import type { Expense } from '../type'
import { ExpenseRepository } from './expenseRepository'

jest.mock('fs')

describe('ExpenseRepository', () => {
  let repository: ExpenseRepository

  const mockFs = jest.mocked(fs)

  beforeEach(() => {
    mockFs.writeFileSync.mockReset()
    mockFs.existsSync.mockReset()
    mockFs.readFileSync.mockReset()
    repository = new ExpenseRepository('expenses.json')
  })

  describe('saveExpense', () => {
    it('expenseが保存されること', () => {
      const expense: Expense = {
        groupName: 'テストグループ1',
        expenseName: 'ディナー',
        payer: '一郎',
        amount: 10000,
      }
      mockFs.existsSync.mockReturnValueOnce(true)
      mockFs.readFileSync.mockReturnValueOnce(JSON.stringify([]))
      repository.saveExpense(expense)
      expect(mockFs.writeFileSync).toHaveBeenCalledWith(
        'expenses.json',
        JSON.stringify([expense]),
      )
    })
  })

  describe('loadExpense', () => {
    it('ファイルが存在する場合、Expense配列が返却される', () => {
      const expenses: Expense[] = [
        {
          groupName: 'テストグループ1',
          expenseName: 'ディナー',
          payer: '一郎',
          amount: 10000,
        },
        {
          groupName: 'テストグループ2',
          expenseName: 'ランチ',
          payer: '二郎',
          amount: 3000,
        },
      ]
      mockFs.existsSync.mockReturnValueOnce(true)
      mockFs.readFileSync.mockReturnValueOnce(JSON.stringify(expenses))
      const result = repository.loadExpenses()
      expect(result).toEqual(expenses)
      expect(mockFs.existsSync).toHaveBeenCalledWith('expenses.json')
      expect(mockFs.readFileSync).toHaveBeenCalledWith('expenses.json', 'utf8')
    })

    it('ファイルが存在しない場合、空の配列が返却される', () => {
      mockFs.existsSync.mockReturnValueOnce(false)
      const result = repository.loadExpenses()
      expect(result).toEqual([])
      expect(mockFs.existsSync).toHaveBeenCalledWith('expenses.json')
      expect(mockFs.readFileSync).toHaveBeenCalledTimes(0)
    })
  })
})