import app from '../app'
import supertest from 'supertest'
import sakiewkaCrypto from 'sakiewka-crypto'

const chai = require('chai')
chai.use(require('chai-uuid'))
const expect = chai.expect

declare var jest: any

const { constants, user } = sakiewkaCrypto

// @ts-ignore
user.login = jest.fn(() => {
  throw new Error('Failed to login a user')
})

describe('server', () => {
  it('should contain error uuid as id on error', async () => {
    const response = await supertest(app)
      .post(`/${constants.BASE_API_PATH}/user/login`)
      .send({
        login: 'testLogin',
        password: 'testPassword'
      })

    expect(response.status).to.be.equal(500)
    expect(response.body.error).to.haveOwnProperty('id')
    expect(response.body.error.id).to.be.a.uuid()
  })
})