import { HttpException } from './http-exception'

describe('HttpException package', () => {
  let status: number
  let message: string
  let path: string | undefined

  beforeEach(() => {
    status = 200
    message = 'Test Message'
    path = 'Test Path'
  })

  it('should extends from Error class', () => {
    expect(HttpException.prototype instanceof Error).toBeTruthy()
  })

  it('should contains status and message properties', () => {
    path = undefined

    const httpException = new HttpException(status, message)

    expect(httpException).toHaveProperty('status', status)
    expect(httpException).toHaveProperty('message', message)
    expect(httpException).toHaveProperty('path', path)
  })

  it('should contains status, message and path properties', () => {
    const httpException = new HttpException(status, message, path)

    expect(httpException).toHaveProperty('status', status)
    expect(httpException).toHaveProperty('message', message)
    expect(httpException).toHaveProperty('path', path)
  })

  it('should throw with a specific message', () => {
    const execute = () => {
      throw new HttpException(status, message, path)
    }

    expect(execute).toThrowError(HttpException)
    expect(execute).toThrow(message)
  })

  it('should return an error with specific status, message and path values', () => {
    try {
      throw new HttpException(status, message, path)
    } catch (error) {
      expect(error).toBeInstanceOf(HttpException)
      if (error instanceof HttpException) {
        expect(error.status).toBe(status)
        expect(error.message).toBe(message)
        expect(error.path).toBe(path)
      }
    }
  })
})