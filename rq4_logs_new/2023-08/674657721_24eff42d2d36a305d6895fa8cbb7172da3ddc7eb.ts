import { ValidatorHelper } from './utils/ValidationHelper'

interface StockProps {
  stock: string
  price: number
}

export class Stock {
  private _stock: string
  private _price: number

  constructor(props: StockProps) {
    const validationResult = this.validateInput(props)

    if (validationResult) {
      throw new Error(`Validation error: invalid ${validationResult}`)
    }

    this._stock = props.stock
    this._price = props.price
  }

  get stock(): string {
    return this._stock
  }

  get price(): number {
    return this._price
  }

  private validateInput({ price, stock }: StockProps): string | null {
    if (!ValidatorHelper.checkPositiveInteger(price)) {
      return 'price'
    }
    if (typeof stock !== 'string') {
      return 'stock'
    }

    return null
  }
}