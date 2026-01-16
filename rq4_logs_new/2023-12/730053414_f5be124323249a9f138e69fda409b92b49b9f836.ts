import type express from 'express'
import fs from 'fs'
import request from 'supertest'

import { createApp } from '../src/app'
import type { Expense, Group, Settlement } from '../src/type'

const GROUP_FILE_PATH = '../data/integration/groups.json'
const EXPENSE_FILE_PATH = '../data/integration/expenses.json'

const testGroups: Group[] = [
  { name: 'group1', members: ['一郎', '二郎', '三郎'] },
  { name: 'group2', members: ['太郎', '花子'] },
]

const testExpenses: Expense[] = [
  { groupName: 'group1', expenseName: 'ランチ', payer: '一郎', amount: 1000 },
  { groupName: 'group2', expenseName: 'ディナー', payer: '太郎', amount: 3000 },
]

describe('Integration test', () => {
  let app: express.Express

  beforeEach(() => {
    fs.writeFileSync(GROUP_FILE_PATH, JSON.stringify(testGroups))
    fs.writeFileSync(EXPENSE_FILE_PATH, JSON.stringify(testExpenses))

    app = createApp(GROUP_FILE_PATH, EXPENSE_FILE_PATH)
  })

  describe('GET /groups', () => {
    it('すべてのグループが取得できる', async () => {
      const response = await request(app).get('/groups')
      expect(response.status).toBe(200)
      expect(response.body).toEqual(testGroups)
    })
  })

  describe('GET /groups/:name', () => {
    it('グループ名を指定したグループが取得できる', async () => {
      const response = await request(app).get(`/groups/${testGroups[0].name}`)
      expect(response.status).toBe(200)
      expect(response.body).toEqual(testGroups[0])
    })
  })

  describe('POST /groups', () => {
    it('グループが追加できる', async () => {
      const newGroup: Group = { name: 'group3', members: ['Alice', 'Bob'] }
      const response = await request(app).post('/groups').send(newGroup)
      expect(response.status).toBe(200)
      expect(response.text).toEqual('グループの作成が成功しました')
    })
  })

  describe('GET /settlements/:groupName', () => {
    it('指定したグループ名のグループの精算リストを取得できる', async () => {
      const expected: Settlement[] = [
        { from: '花子', to: '太郎', amount: 1500 },
      ]
      const response = await request(app).get(
        `/settlements/${testGroups[1].name}`,
      )
      expect(response.status).toBe(200)
      expect(response.body).toEqual(expected)
    })
  })

  describe('POST /expenses', () => {
    it('支出が登録できる', async () => {
      const newExpense: Expense = {
        groupName: 'group1',
        expenseName: '旅行',
        payer: '二郎',
        amount: 10000,
      }
      const response = await request(app).post('/expenses').send(newExpense)
      expect(response.status).toBe(200)
      expect(response.text).toBe('支出が登録されました')
    })
  })
})