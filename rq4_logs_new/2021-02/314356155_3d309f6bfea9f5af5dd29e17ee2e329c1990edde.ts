import { HttpResponse, HttpRequest } from '@/web-controllers/ports'

import { RegisterUserOnMailingList } from '@/usecases/register-user-on-maling-list'
import { UserData } from '@/entities'
import { UserRepository } from '@/usecases/register-user-on-maling-list/ports'
import { InMemoryUserRepository } from '@test/usecases/register-user-on-maling-list/repository'
import { RegisterUserController } from '@/web-controllers/register-user-controller'

describe('Register user web controller', () => {
  test('should return status code 201 when request contains a valid user data', async () => {
    const request: HttpRequest = {
      body: {
        name: 'Any name',
        email: 'any@mail.com'
      }
    }

    const users: UserData[] = []
    const repo: UserRepository = new InMemoryUserRepository(users)
    const usecase: RegisterUserOnMailingList = new RegisterUserOnMailingList(repo)
    const controller: RegisterUserController = new RegisterUserController(usecase)
    const response: HttpResponse = await controller.handle(request)
    expect(response.statusCode).toEqual(201)
    expect(response.body).toEqual(request.body)
  })
})