/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, expect, it } from 'vitest'
import { InvestmentTransacition } from './InvestmentTransaction'

const validInvestmentTransactionData = {
  event: InvestmentTransacition.INVESTMENT_TRANSACTION_EVENT,
  stock: 'TEST',
  price: 4000,
  quantity: 100,
}

describe('Investment transaction unit tests', () => {
  it('should be able to create a new investment transaction', async () => {
    const investmentTransaction = new InvestmentTransacition(
      validInvestmentTransactionData,
    )

    expect(investmentTransaction).toEqual(
      expect.objectContaining({
        _event: validInvestmentTransactionData.event,
        _stock: validInvestmentTransactionData.stock,
        _price: validInvestmentTransactionData.price,
        _quantity: validInvestmentTransactionData.quantity,
        _createdAt: expect.any(Number),
      }),
    )
  })

  it('should not be able to create a new investment transaction with invalid event', async () => {
    const EXPECTED_ERROR = 'Validation error: invalid event'

    expect(() => {
      const investmentTransaction = new InvestmentTransacition({
        ...validInvestmentTransactionData,
        event: 'TEST',
      })
    }).toThrowError(EXPECTED_ERROR)
    expect(() => {
      const investmentTransaction = new InvestmentTransacition({
        ...validInvestmentTransactionData,
        event: undefined,
      })
    }).toThrowError(EXPECTED_ERROR)
  })

  it('should not be able to create a new investment transaction with invalid quantity', async () => {
    const EXPECTED_ERROR = 'Validation error: invalid quantity'

    expect(() => {
      const investmentTransaction = new InvestmentTransacition({
        ...validInvestmentTransactionData,
        quantity: undefined,
      })
    }).toThrowError(EXPECTED_ERROR)
  })

  it('should not be able to create a new investment transaction with invalid price', async () => {
    const EXPECTED_ERROR = 'Validation error: invalid price'

    expect(() => {
      const investmentTransaction = new InvestmentTransacition({
        ...validInvestmentTransactionData,
        price: undefined,
      })
    }).toThrowError(EXPECTED_ERROR)
  })

  it('should not be able to create a new investment transaction with invalid stock', async () => {
    const EXPECTED_ERROR = 'Validation error: invalid stock'

    expect(() => {
      const investmentTransaction = new InvestmentTransacition({
        ...validInvestmentTransactionData,
        stock: undefined,
      })
    }).toThrowError(EXPECTED_ERROR)
  })
})