export class UnexpectedError extends Error {
  constructor () {
    super('Algo deu errado, tente novamente!')
    this.name = 'UnexpectedError'
  }
}