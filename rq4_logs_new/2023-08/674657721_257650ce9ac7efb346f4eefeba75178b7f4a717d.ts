import { InvestmentTransacition } from '@/domain/entities/InvestmentTransaction'
import { TransferTransacition } from '@/domain/entities/TransferTransaction'
import { ITransactionsRepository } from '@/domain/interfaces/ITransactionsRepository'

export class TransactionsRepository implements ITransactionsRepository {
  private transactions: (InvestmentTransacition | TransferTransacition)[]

  constructor() {
    this.transactions = []
  }

  async createTransfer(transferTransaction: TransferTransacition) {
    this.transactions.push(transferTransaction)
    return transferTransaction
  }

  async createInvestment(investmentTransaction: InvestmentTransacition) {
    this.transactions.push(investmentTransaction)
    return investmentTransaction
  }

  async getBalance(accountNumber: number) {
    const totalBalance = this.transactions.reduce((balance, transaction) => {
      if (transaction.accountNumber === accountNumber) {
        if (
          transaction.event === TransferTransacition.TRANSFER_TRANSACTION_EVENT
        ) {
          const transfer = transaction as TransferTransacition
          const transferAmount = transfer.amount
          return balance + transferAmount
        } else {
          const investment = transaction as InvestmentTransacition
          const investmentAmount = investment.price * investment.quantity
          return balance - investmentAmount
        }
      } else {
        return balance
      }
    }, 0)

    return totalBalance
  }
}