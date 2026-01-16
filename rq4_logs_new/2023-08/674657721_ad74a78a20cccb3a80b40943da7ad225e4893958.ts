import { UserAccountRepository } from '@/providers/database/dynamodb/UserAccountRepository'
import { TransactionsRepository } from '@/providers/database/dynamodb/TransactionsRepository'
import { StockRepository } from '@/providers/database/dynamodb/StockRepository'
import { CreateInvestmentUseCase } from '@/domain/use-cases/create-investment'

export function makeInvestmentUseCase() {
  const usersAccountRepository = new UserAccountRepository()
  const transactionsRepository = new TransactionsRepository()
  const stockRepository = new StockRepository()
  const createInvestmentUseCase = new CreateInvestmentUseCase(
    transactionsRepository,
    stockRepository,
    usersAccountRepository,
  )

  return createInvestmentUseCase
}