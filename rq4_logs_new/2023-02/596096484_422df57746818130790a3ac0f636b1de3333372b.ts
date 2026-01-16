import { RedGin, propReflect } from 'redgin'
import { Product } from './products'

class ProductAlerts extends RedGin {
  product = propReflect<Product>(null)
  render() {
    return `
      ${ this.product && this.product.price > 700 ? ` 
        <p>
          <button type="button">Notify Me</button>
        </p>      
      ` : `` }

      `
  }
}

customElements.define('product-alerts', ProductAlerts)