import { UserAccountRepository } from '@/providers/database/in-memory/UserAccountRepository'
import { beforeAll, describe, expect, it } from 'vitest'
import { UserAccount } from '../entities/UserAccount'
import { CreateTransferUseCase } from './create-transfer'
import { TransactionsRepository } from '@/providers/database/in-memory/TransactionsRepository'
import { TransferTransacition } from '../entities/TransferTransaction'
import { AppError } from '../errors/AppError'

let transactionsRepository: TransactionsRepository
let userAccountRepository: UserAccountRepository
let createTransferUseCase: CreateTransferUseCase

describe('Create transfer transaction account integration tests', () => {
  beforeAll(async () => {
    userAccountRepository = new UserAccountRepository()
    transactionsRepository = new TransactionsRepository()
    createTransferUseCase = new CreateTransferUseCase(
      transactionsRepository,
      userAccountRepository,
    )
  })

  it('should be able to create a new transfer transaction', async () => {
    const userAccount = new UserAccount('36577946035')
    userAccount.accountNumber = 1
    await userAccountRepository.create(userAccount)

    const validTransferTransactionData = {
      event: TransferTransacition.TRANSFER_TRANSACTION_EVENT,
      amount: 1000,
      target: {
        account: '1',
        bank: TransferTransacition.TARGET_BANK,
        branch: TransferTransacition.TARGET_BRANCH,
      },
      origin: { bank: '0002', branch: '353', cpf: '36577946035' },
    }

    const transferTransaction = new TransferTransacition(
      validTransferTransactionData,
    )

    const createdTransfer = await createTransferUseCase.execute(
      transferTransaction,
    )

    expect(createdTransfer).toEqual(
      expect.objectContaining({
        _accountNumber: transferTransaction.accountNumber,
      }),
    )
  })

  it('should not be able to create a new transfer for a non existing account', async () => {
    const validTransferTransactionData = {
      event: TransferTransacition.TRANSFER_TRANSACTION_EVENT,
      amount: 1000,
      target: {
        account: '2',
        bank: TransferTransacition.TARGET_BANK,
        branch: TransferTransacition.TARGET_BRANCH,
      },
      origin: { bank: '0002', branch: '353', cpf: '36577946035' },
    }

    const transferTransaction = new TransferTransacition(
      validTransferTransactionData,
    )

    expect(async () => {
      const createdTransfer = await createTransferUseCase.execute(
        transferTransaction,
      )
    }).rejects.toThrowError(AppError)
  })
})