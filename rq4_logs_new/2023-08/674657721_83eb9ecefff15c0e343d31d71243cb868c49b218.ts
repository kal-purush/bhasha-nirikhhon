/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, expect, it } from 'vitest'
import { TransferTransacition } from './TransferTransaction'

const VALID_CPF = '36577946035'
const INVALID_CPF = '08654098745'

describe('Transfer transaction unit tests', () => {
  it('should be able to create a new transfer transaction', async () => {
    const event = 'TRANSFER'
    const amount = 1000
    const target = { account: '123456789', bank: '352', branch: '0001' }
    const origin = { bank: '0002', branch: '353', cpf: VALID_CPF }
    const transferTransactionData = {
      event,
      amount,
      target,
      origin,
    }

    const transferTransaction = new TransferTransacition(
      transferTransactionData,
    )

    expect(transferTransaction).toEqual(
      expect.objectContaining({
        _event: event,
        _amount: amount,
        _target: expect.objectContaining(target),
        _origin: expect.objectContaining(origin),
      }),
    )
  })

  it('should not be able to create a new transfer transaction with invalid amount', async () => {
    const transferTransactionData = {
      event: undefined as any,
      origin: undefined as any,
      target: undefined as any,
    }

    const EXPECTED_ERROR = 'Validation error: invalid amount'

    expect(() => {
      const transferTransaction = new TransferTransacition({
        ...transferTransactionData,
        amount: undefined as any,
      })
    }).toThrowError(EXPECTED_ERROR)

    expect(() => {
      const transferTransaction = new TransferTransacition({
        ...transferTransactionData,
        amount: 0 as any,
      })
    }).toThrowError(EXPECTED_ERROR)
  })

  it('should not be able to create a new transfer transaction with invalid event', async () => {
    const transferTransactionData = {
      amount: 1000,
      origin: undefined as any,
      target: undefined as any,
    }

    const EXPECTED_ERROR = 'Validation error: invalid event'

    expect(() => {
      const transferTransaction = new TransferTransacition({
        ...transferTransactionData,
        event: undefined as any,
      })
    }).toThrowError(EXPECTED_ERROR)

    expect(() => {
      const transferTransaction = new TransferTransacition({
        ...transferTransactionData,
        event: 'TEST' as any,
      })
    }).toThrowError(EXPECTED_ERROR)
  })

  it('should not be able to create a new transfer transaction with invalid origin', async () => {
    const transferTransactionData = {
      amount: 1000,
      event: 'TRANSFER',
      target: undefined as any,
    }

    const EXPECTED_ERROR = 'Validation error: invalid origin'

    expect(() => {
      const transferTransaction = new TransferTransacition({
        ...transferTransactionData,
        origin: undefined as any,
      })
    }).toThrowError(EXPECTED_ERROR)

    expect(() => {
      const transferTransaction = new TransferTransacition({
        ...transferTransactionData,
        origin: { bank: undefined as any, branch: '1', cpf: VALID_CPF },
      })
    }).toThrowError(EXPECTED_ERROR)

    expect(() => {
      const transferTransaction = new TransferTransacition({
        ...transferTransactionData,
        origin: { bank: '1', branch: undefined as any, cpf: VALID_CPF },
      })
    }).toThrowError(EXPECTED_ERROR)

    expect(() => {
      const transferTransaction = new TransferTransacition({
        ...transferTransactionData,
        origin: { bank: '1', branch: '1', cpf: undefined as any },
      })
    }).toThrowError(EXPECTED_ERROR)
  })

  it('should not be able to create a new transfer transaction with invalid target', async () => {
    const transferTransactionData = {
      amount: 1000,
      event: 'TRANSFER',
      origin: { bank: '0002', branch: '353', cpf: VALID_CPF },
    }

    const EXPECTED_ERROR = 'Validation error: invalid target'

    expect(() => {
      const transferTransaction = new TransferTransacition({
        ...transferTransactionData,
        target: undefined as any,
      })
    }).toThrowError(EXPECTED_ERROR)

    expect(() => {
      const transferTransaction = new TransferTransacition({
        ...transferTransactionData,
        target: { bank: undefined as any, branch: '0001', account: '1' },
      })
    }).toThrowError(EXPECTED_ERROR)

    expect(() => {
      const transferTransaction = new TransferTransacition({
        ...transferTransactionData,
        target: { bank: '1', branch: undefined as any, account: '1' },
      })
    }).toThrowError(EXPECTED_ERROR)

    expect(() => {
      const transferTransaction = new TransferTransacition({
        ...transferTransactionData,
        target: { bank: '1', branch: '1', account: undefined as any },
      })
    }).toThrowError(EXPECTED_ERROR)
  })
})