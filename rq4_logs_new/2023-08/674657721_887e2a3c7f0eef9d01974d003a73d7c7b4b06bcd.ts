import { ValidatorHelper } from './utils/ValidationHelper'

interface InvestmentTransactionProps {
  event: string
  stock: string
  price: number
  quantity: number
}

export class InvestmentTransacition {
  static INVESTMENT_TRANSACTION_EVENT = 'INVESTMENT'

  private _event: string
  private _stock: string
  private _price: number
  private _quantity: number
  private _createdAt: number

  constructor(props: InvestmentTransactionProps) {
    const validationResult = this.validateInput(props)

    if (validationResult) {
      throw new Error(`Validation error: invalid ${validationResult}`)
    }

    this._event = props.event
    this._stock = props.stock
    this._price = props.price
    this._quantity = props.quantity
    this._createdAt = new Date().getTime()
  }

  get stock() {
    return this._stock
  }

  get price() {
    return this._price
  }

  get quantity() {
    return this._quantity
  }

  get event() {
    return this._event
  }

  get createdAt() {
    return this._createdAt
  }

  private validateInput({
    event,
    stock,
    price,
    quantity,
  }: InvestmentTransactionProps): string | null {
    if (event !== InvestmentTransacition.INVESTMENT_TRANSACTION_EVENT) {
      return 'event'
    }
    if (!ValidatorHelper.checkPositiveInteger(quantity)) {
      return 'quantity'
    }
    if (!ValidatorHelper.checkPositiveInteger(price)) {
      return 'price'
    }
    if (typeof stock !== 'string') {
      return 'stock'
    }
    return null
  }
}