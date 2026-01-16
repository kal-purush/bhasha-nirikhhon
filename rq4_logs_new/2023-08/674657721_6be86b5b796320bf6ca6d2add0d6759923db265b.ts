import { UserAccount } from '../entities/UserAccount'
import { AppError } from '../errors/AppError'
import { IUserAccountRepository } from '../interfaces/IUserAccountRepository'

export class AuthenticateUserUseCase {
  private userAccountRepository: IUserAccountRepository

  constructor(userAccountRepository: IUserAccountRepository) {
    this.userAccountRepository = userAccountRepository
  }

  async execute(cpf: string) {
    const account = await this.userAccountRepository.findByCpf(cpf)
    if (!account) {
      throw new AppError('Invalid credentials')
    }
    return account
  }
}