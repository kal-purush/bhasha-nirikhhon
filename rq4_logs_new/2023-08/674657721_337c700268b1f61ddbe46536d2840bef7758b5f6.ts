/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, expect, it } from 'vitest'
import { Stock } from './Stock'

const validStockData = {
  stock: 'TEST',
  price: 4000,
}

describe('Stock unit tests', () => {
  it('should be able to create a new stock', async () => {
    const stock = new Stock(validStockData)

    expect(stock).toEqual(
      expect.objectContaining({
        _stock: validStockData.stock,
        _price: validStockData.price,
      }),
    )
  })

  it('should not be able to create a new stock with invalid price', async () => {
    const EXPECTED_ERROR = 'Validation error: invalid price'

    expect(() => {
      const stock = new Stock({
        ...validStockData,
        price: undefined,
      })
    }).toThrowError(EXPECTED_ERROR)
  })

  it('should not be able to create a new stock with invalid stock', async () => {
    const EXPECTED_ERROR = 'Validation error: invalid stock'

    expect(() => {
      const stock = new Stock({
        ...validStockData,
        stock: undefined,
      })
    }).toThrowError(EXPECTED_ERROR)
  })
})