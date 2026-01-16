import { UserAccountRepository } from '@/providers/database/in-memory/UserAccountRepository'
import { beforeAll, describe, expect, it } from 'vitest'
import { UserAccount } from '../entities/UserAccount'
import { CreateUserAccountUseCase } from './create-user-account'
import { AuthenticateUserUseCase } from './authenticate-user'

let userAccountRepository: UserAccountRepository
let authenticateUserCase: AuthenticateUserUseCase

describe('Authenticate user integration tests', () => {
  beforeAll(() => {
    userAccountRepository = new UserAccountRepository()
    authenticateUserCase = new AuthenticateUserUseCase(userAccountRepository)
  })

  it('should be able to authenticate a user', async () => {
    const userAccount = new UserAccount('36577946035')
    userAccount.accountNumber = 1

    await userAccountRepository.create(userAccount)

    const authenticatedUser = await authenticateUserCase.execute(
      userAccount.cpf,
    )

    expect(authenticatedUser).toEqual({
      _cpf: userAccount.cpf,
      _createdAt: expect.any(Number),
      _accountNumber: userAccount.accountNumber,
    })
  })

  it('should not be able to authenticate a user with invalid credentials', async () => {
    expect(
      async () => await authenticateUserCase.execute('123456789'),
    ).rejects.toThrowError()
  })
})