import { expect } from 'chai'
import app from '../../../app'
import supertest from 'supertest'
import { constants } from 'sakiewka-crypto'
// @ts-ignore
const { user } = app.sakiewkaApi

// @ts-ignore
const mockFn = jest.fn().mockResolvedValue('balbace')

user.balance = mockFn

describe('/user/balance', () => {
  beforeEach(() => {
    mockFn.mockClear()
  })

  it('should not accept request with missing header', async () => {
    const response = await supertest(app)
      .get(`/${constants.BASE_API_PATH}/user/balance`)
      .send({ fiatCurrency: 'USD' })

    expect(response.status).to.be.equal(400)
    expect(response.body.errors[0].message).to.be.equal('Request header Authorization is required.')
  })

  it('should not accept request without authorisation', async () => {
    const response = await supertest(app)
      .get(`/${constants.BASE_API_PATH}/user/balance`)
      .send({ fiatCurrency: 'USD' })

    expect(response.status).to.be.equal(400)
    expect(response.body.errors[0].message).to.be.equal('Request header Authorization is required.')
  })

  it('should fetch user balance', async () => {
    const token = 'testToken'

    const response = await supertest(app)
      .get(`/${constants.BASE_API_PATH}/user/balance`)
      .set('Authorization', `Bearer ${token}`)
      .send({ fiatCurrency: 'USD' })

    const callArgs = mockFn.mock.calls[0]

    expect(response.status).to.be.equal(200)
    const data = response.body.data
    expect(data).to.eq('balbace')
    expect(callArgs[0]).to.eq(`Bearer ${token}`)
  })

  it('should not accept extra parameters', async () => {
    const token = 'testToken'

    const response = await supertest(app)
      .get(`/${constants.BASE_API_PATH}/user/balance`)
      .set('Authorization', `Bearer ${token}`)
      .send({
        fiatCurrency: 'USD',
        extraProp: 'test'
      })

    expect(response.status).to.be.equal(400)
    expect(response.body.errors[0].message).to.be.equal('"extraProp" is not allowed')
  })
})