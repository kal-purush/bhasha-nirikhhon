import { UserAccountRepository } from '@/providers/database/in-memory/UserAccountRepository'
import { beforeAll, describe, expect, it } from 'vitest'
import { UserAccount } from '../entities/UserAccount'
import { CreateUserAccountUseCase } from './create-user-account'

let userAccountRepository: UserAccountRepository
let userAccountUserCase: CreateUserAccountUseCase

describe('Create user account integration tests', () => {
  beforeAll(() => {
    userAccountRepository = new UserAccountRepository()
    userAccountUserCase = new CreateUserAccountUseCase(userAccountRepository)
  })

  it('should be able to create a new user account', async () => {
    const newUserAccount = new UserAccount('36577946035')
    const createdUserAccount = await userAccountUserCase.execute(newUserAccount)

    expect(createdUserAccount).toEqual({
      _cpf: newUserAccount.cpf,
      _createdAt: expect.any(Number),
      _accountNumber: 1,
    })
  })

  it('should not be able to create a new user account with the same cpf', async () => {
    const newUserAccount = new UserAccount('36577946035')

    expect(
      async () => await userAccountUserCase.execute(newUserAccount),
    ).rejects.toThrowError()
  })

  it('should be able to create a new user account with incremented account number', async () => {
    const newUserAccount = new UserAccount('89186073001')
    const createdUserAccount = await userAccountUserCase.execute(newUserAccount)

    expect(createdUserAccount).toEqual({
      _cpf: newUserAccount.cpf,
      _createdAt: expect.any(Number),
      _accountNumber: 2,
    })
  })
})