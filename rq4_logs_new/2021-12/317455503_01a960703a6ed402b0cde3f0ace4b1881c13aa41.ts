import RestClient from './lib/rest_client'
import user from './lib/user'
import cart from './lib/cart'
import order from './lib/order'
import stock from './lib/stock'
import contact from './lib/contact'
import wishlist from './lib/wishlist'
import stockAlert from './lib/stock_alert'
import newsletter from './lib/newsletter'
import address from './lib/address'

const MAGENTO_API_VERSION = 'V1'

export type Magento1Client = any

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default function (options: any): any {
  const instance: any = {
    addMethods (key, module) {
      const client = RestClient(options)
      if (module) {
        if (this[key]) {
          this[key] = Object.assign(this[key], module(client))
        } else {
          this[key] = module(client)
        }
      }
    }
  }

  options.version = MAGENTO_API_VERSION

  const client = RestClient(options)

  instance.user = user(client)
  instance.cart = cart(client)
  instance.order = order(client)
  instance.stock = stock(client)
  instance.contact = contact(client)
  instance.wishlist = wishlist(client)
  instance.stockAlert = stockAlert(client)
  instance.newsletter = newsletter(client)
  instance.address = address(client)

  return instance
}