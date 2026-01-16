import { InvestmentTransacition } from '../entities/InvestmentTransaction'
import { AppError } from '../errors/AppError'
import { IStockRepository } from '../interfaces/IStockRepository'
import { ITransactionsRepository } from '../interfaces/ITransactionsRepository'
import { IUserAccountRepository } from '../interfaces/IUserAccountRepository'

export class CreateInvestmentUseCase {
  constructor(
    private transactionsRepository: ITransactionsRepository,
    private stockRepository: IStockRepository,
    private userAccountRepository: IUserAccountRepository,
  ) {}

  async execute(investmentTransaction: InvestmentTransacition, cpf: string) {
    const userAccount = await this.userAccountRepository.findByCpf(cpf)
    if (!userAccount) {
      throw new AppError('Invalid credentials')
    }

    const stock = await this.stockRepository.findByName(
      investmentTransaction.stock,
    )
    if (!stock) {
      throw new AppError('Stock does not exist')
    }

    investmentTransaction.accountNumber = userAccount.accountNumber
    investmentTransaction.price = stock.price

    const createdInvestment =
      await this.transactionsRepository.createInvestment(investmentTransaction)

    return createdInvestment
  }
}