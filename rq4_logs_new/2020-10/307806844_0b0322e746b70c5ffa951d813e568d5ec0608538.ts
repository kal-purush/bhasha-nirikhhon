import { createServer, Request, dropDB, disconnectDB, matchBody, hasBodyProperty } from './utils'
import { generateToken } from '../src/models/auth'

describe('Users', () => {
  let request: Request

  beforeAll(async () => {
    request = await createServer()
  })

  beforeEach(async () => {
    await dropDB()
    await request.post('/register').send(userJson)
  })

  afterAll(async () => {
    await disconnectDB()
  })

  test('Register', () =>
    request.post('/register')
      .send({ ...userJson, username: 'RANDOM', profile: { nickname: 'NICK' } })
      .expect(200)
      .then(matchBody({ nickname: 'NICK' }))
  )

  test('Login', () =>
    request.post('/login')
      .send(userJson.credentials)
      .expect(200)
      .then(hasBodyProperty('token'))
  )

  test('Profile', () =>
    request.get(`/profile?access_token=${token}`)
      .send(userJson.credentials)
      .expect(200)
      .then(matchBody(userJson.profile))
  )

  // test('Required error', () =>
  //   request.post('/users')
  //     .send({})
  //     .expect(400, 'Path `userId` is required.')
  // )


})


const username = 'USERNAME'

const userJson = {
  credentials: {
    username,
    password: 'PASSWORD',
    parentName: 'string',
    parentCUIL: 'string',

  },
  profile: {
    nickName: username,
    avatarURL: 'string'
  }
}

const token = generateToken({ username })