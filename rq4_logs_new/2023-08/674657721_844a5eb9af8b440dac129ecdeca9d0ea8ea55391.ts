import { UserAccountRepository } from '@/providers/database/in-memory/UserAccountRepository'
import { beforeEach, describe, expect, it } from 'vitest'
import { UserAccount } from '../entities/UserAccount'
import { TransactionsRepository } from '@/providers/database/in-memory/TransactionsRepository'
import { AppError } from '../errors/AppError'
import { StockRepository } from '@/providers/database/in-memory/StockRepository'
import { CreateInvestmentUseCase } from './create-investment'
import { InvestmentTransacition } from '../entities/InvestmentTransaction'

const VALID_CPF = '36577946035'

let stockRepository: StockRepository
let transactionsRepository: TransactionsRepository
let userAccountRepository: UserAccountRepository
let createInvestmentUseCase: CreateInvestmentUseCase

describe('Create investment transaction integration tests', () => {
  beforeEach(async () => {
    stockRepository = new StockRepository()
    userAccountRepository = new UserAccountRepository()
    transactionsRepository = new TransactionsRepository()
    createInvestmentUseCase = new CreateInvestmentUseCase(
      transactionsRepository,
      stockRepository,
      userAccountRepository,
    )
  })

  it('should be able to create a new investment transaction', async () => {
    const userAccount = new UserAccount(VALID_CPF)
    userAccount.accountNumber = 1
    await userAccountRepository.create(userAccount)

    const validInvestmentTransactionData = {
      event: InvestmentTransacition.INVESTMENT_TRANSACTION_EVENT,
      quantity: 100,
      stock: 'PETR4',
    }

    const investmentTransaction = new InvestmentTransacition(
      validInvestmentTransactionData,
    )

    const createdInvestment = await createInvestmentUseCase.execute(
      investmentTransaction,
      userAccount.cpf,
    )

    expect(createdInvestment).toEqual(
      expect.objectContaining({
        _accountNumber: investmentTransaction.accountNumber,
      }),
    )
  })

  it('should not be able to create a new investment transaction with unexisting stock', async () => {
    const userAccount = new UserAccount(VALID_CPF)
    userAccount.accountNumber = 1
    await userAccountRepository.create(userAccount)

    const nonExistingStock = 'PETR1'

    const validInvestmentTransactionData = {
      event: InvestmentTransacition.INVESTMENT_TRANSACTION_EVENT,
      quantity: 100,
      stock: nonExistingStock,
    }

    const investmentTransaction = new InvestmentTransacition(
      validInvestmentTransactionData,
    )

    expect(async () => {
      const createdInvestment = await createInvestmentUseCase.execute(
        investmentTransaction,
        userAccount.cpf,
      )
    }).rejects.toThrowError(AppError)
  })

  it('should not be able to create a new transfer with invalid credentials', async () => {
    const validInvestmentTransactionData = {
      event: InvestmentTransacition.INVESTMENT_TRANSACTION_EVENT,
      quantity: 100,
      stock: 'PETR4',
    }

    const investmentTransaction = new InvestmentTransacition(
      validInvestmentTransactionData,
    )

    const invalidCpf = '123456789'

    expect(async () => {
      const createdInvestment = await createInvestmentUseCase.execute(
        investmentTransaction,
        invalidCpf,
      )
    }).rejects.toThrowError(AppError)
  })
})