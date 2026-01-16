import { MongoHelper } from '@/external/repositories/mongodb/helper'
import { MongodbUserRepository } from '@/external/repositories/mongodb'

describe('Mongodb User Reposity', () => {
  beforeAll(async () => {
    await MongoHelper.connect(process.env.MONGO_URL)
  })

  afterAll(async () => {
    await MongoHelper.disconnect()
  })

  beforeEach(async () => {
    await MongoHelper.clearCollection('users')
  })

  test('when user is added, it should exist', async () => {
    const userReposity = new MongodbUserRepository()
    const user = {
      name: 'any name',
      email: 'any@email.com'
    }
    await userReposity.add(user)
    expect(await userReposity.exists(user)).toBeTruthy()
  })

  test('final add users should return all added users', async () => {
    const userReposity = new MongodbUserRepository()
    await userReposity.add({
      name: 'first name',
      email: 'first@email.com'
    })
    await userReposity.add({
      name: 'second name',
      email: 'second@email.com'
    })
    const users = await userReposity.findAllUsers()
    expect(users[0].name).toEqual('first name')
    expect(users[1].name).toEqual('second name')
  })
})