const ApiError = require('../../../src/lib/errors/ApiError')
const authMiddleware = require('../../../src/middlewares/auth')

describe('Authorization Middleware', () => {
  it('should not call #next with [error] argument when provided valid auth token', () => {
    const nextMock = jest.fn()
    authMiddleware(
      { headers: { 'x-api-key': 'some-valid-token' } },
      {},
      nextMock
    )
    expect(nextMock.mock.calls[0][0]).toBeUndefined()
  })

  it('should call #next with appropriate ApiError argument when NOT provided valid auth token', () => {
    const nextMock = jest.fn()
    authMiddleware(
      { headers: { 'x-api-key': 'some-fake-token' } },
      {},
      nextMock
    )

    const error = nextMock.mock.calls[0][0]
    expect(error).toBeDefined()
    expect(error).toBeInstanceOf(ApiError)
    expect(error.statusCode).toBe(401)
    expect(error.message).toBe('Invalid api key provided')
    expect(error.isOperational).toBe(true)
  })
})