import { TransferTransacition } from '../entities/TransferTransaction'
import { AppError } from '../errors/AppError'
import { ITransactionsRepository } from '../interfaces/ITransactionsRepository'
import { IUserAccountRepository } from '../interfaces/IUserAccountRepository'

export class CreateTransferUseCase {
  constructor(
    private transactionsRepository: ITransactionsRepository,
    private userAccountRepository: IUserAccountRepository,
  ) {}

  async execute(transferTransaction: TransferTransacition) {
    const account = await this.userAccountRepository.findByAccountNumber(
      transferTransaction.accountNumber,
    )
    if (!account) {
      throw new AppError('Account does not exist')
    }

    const createdTransfer = await this.transactionsRepository.createTransfer(
      transferTransaction,
    )
    return createdTransfer
  }
}