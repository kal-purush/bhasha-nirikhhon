import Errors from './errors'

export class PartyError extends Error {
  constructor(public errors: Errors) {
    super('Party encountered an error')
  }

  get isRequestError() {
    return this.errors.isRequestError
  }

  get isBadRequest() {
    return this.errors.isBadRequest
  }

  get isUnauthorized() {
    return this.errors.isUnauthorized
  }

  get isNotFound() {
    return this.errors.isNotFound
  }
}