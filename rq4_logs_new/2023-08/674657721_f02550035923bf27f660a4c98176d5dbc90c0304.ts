import { UserAccountRepository } from '@/providers/database/dynamodb/UserAccountRepository'
import { CreateTransferUseCase } from '@/domain/use-cases/create-transfer'
import { TransactionsRepository } from '@/providers/database/dynamodb/TransactionsRepository'

export function makeTransferUseCase() {
  const usersAccountRepository = new UserAccountRepository()
  const transactionsRepository = new TransactionsRepository()
  const createTransferUseCase = new CreateTransferUseCase(
    transactionsRepository,
    usersAccountRepository,
  )

  return createTransferUseCase
}