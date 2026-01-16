import type { Expense, Settlement } from '../type'
import { calculateSettlements } from './settlements'

describe('calculateSettlements', () => {
  it('精算リストが算出される', () => {
    const expenses: Expense[] = [
      {
        groupName: 'テストグループ1',
        expenseName: '支出1',
        payer: '一郎',
        amount: 300,
      },
      {
        groupName: 'テストグループ1',
        expenseName: '支出2',
        payer: '二郎',
        amount: 100,
      },
    ]
    const groupMembers = ['一郎', '二郎', '三郎']
    const expectedSettlements: Settlement[] = [
      { from: '二郎', to: '一郎', amount: 34 },
      { from: '三郎', to: '一郎', amount: 133 },
    ]
    const result = calculateSettlements(expenses, groupMembers)
    expect(result).toEqual(expectedSettlements)
  })
})