/**
 * Created by sandrobarone on 11.08.15.
 */
module shop {
  'use strict';

  export interface ICartItem {
    product: IProduct
    quantity: number
  }

  export interface ICart {
    item: ICartItem

    total: number
  }

  export class ShoppingCartProvider {
    private restangular:restangular.IService
    private UserLoginService: UserLoginService

    /** @ngInject */
    constructor(Restangular: restangular.IService, UserLoginService: UserLoginService) {
      this.restangular = Restangular;
      this.UserLoginService = UserLoginService;

    }

    addToShoppingCart (updateShoppingCartCB:(cart:ICart) => void, id: string, quty:number): void {
      var cart = {
        productId: id,
        quantity: quty
      };

      this.restangular.one('product').post('cart', cart).then( (cart: ICart) => {
        updateShoppingCartCB(cart)
      });
    }

  }
}