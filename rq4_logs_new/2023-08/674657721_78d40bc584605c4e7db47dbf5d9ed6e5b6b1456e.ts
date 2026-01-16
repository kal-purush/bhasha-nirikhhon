import { UserAccount } from '@/domain/entities/UserAccount'
import { IUserAccountRepository } from '@/domain/interfaces/IUserAccountRepository'

export class UserAccountRepository implements IUserAccountRepository {
  private userAccount: UserAccount[]
  constructor() {
    this.userAccount = []
  }

  async create(userAccount: UserAccount) {
    this.userAccount.push(userAccount)
    return userAccount
  }

  async findByAccountNumber(accountNumber: number) {
    const account = this.userAccount.find(
      (account) => account.accountNumber === accountNumber,
    )
    return account
  }

  async findByCpf(cpf: string) {
    const account = this.userAccount.find((account) => account.cpf === cpf)
    return account
  }

  async findLastAccountNumber() {
    const lastAccountNumber = this.userAccount.length
    return lastAccountNumber
  }
}