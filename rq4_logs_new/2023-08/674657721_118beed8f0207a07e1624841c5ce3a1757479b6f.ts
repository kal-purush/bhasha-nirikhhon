import { describe, expect, it } from 'vitest'
import request from 'supertest'
import { app } from '@/http/app'

const VALID_CPF = '36577946035'

describe('End-to-end tests for create user account and create transaction routes', () => {
  it('should be able to create a user account', async () => {
    const response = await request(app).post('/user/account').send({
      cpf: VALID_CPF,
    })

    expect(response.status).toEqual(201)
  })

  it('should be able to create a transfer', async () => {
    const response = await request(app)
      .post('/transaction/transfer')
      .send({
        event: 'TRANSFER',
        target: {
          bank: '352',
          branch: '0001',
          account: '1',
        },
        origin: {
          bank: '123',
          branch: '123',
          cpf: VALID_CPF,
        },
        amount: 5000,
      })

    expect(response.status).toEqual(201)
  })

  it('should be able to create a investment', async () => {
    const response = await request(app).post('/transaction/investment').send({
      event: 'INVESTMENT',
      stock: 'PETR4',
      quantity: 100,
      cpf: VALID_CPF,
    })

    expect(response.status).toEqual(201)
  })
})