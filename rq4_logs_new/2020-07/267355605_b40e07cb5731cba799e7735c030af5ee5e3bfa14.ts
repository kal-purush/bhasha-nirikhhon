import { Injectable } from "@angular/core";
import { Product } from '@tcode/api-interface';
import { BehaviorSubject } from 'rxjs';


@Injectable({
  providedIn: "root"
})
export class CartService {
  constructor() {
  }

  private cartCount = new BehaviorSubject(0);
  public cartCount$ = this.cartCount.asObservable()

  addItem(product: Product, count: number) {
    const itemsInCart = JSON.parse(localStorage.getItem('Mycart'));
    if(itemsInCart){
      const existingProductInCart = itemsInCart.products[product.id];
      if(existingProductInCart){
        localStorage.setItem('Mycart', JSON.stringify({
          products: {
            ...itemsInCart.products,
            [product.id]: {
              item: product,
              quantity: existingProductInCart.quantity += count
            }
          }
        }));
        this.updateCartItemCount();
      } else {
        localStorage.setItem('Mycart', JSON.stringify({
          products: {
            ...itemsInCart.products,
            [product.id]: {
              item: product,
              quantity: count
            }
          }
        }));
        this.updateCartItemCount();
      }
    } else {
      localStorage.setItem('Mycart', JSON.stringify({
        products: {
          [product.id]: {
            item: product,
            quantity: count
          }
        }
      }));
      this.updateCartItemCount();
    }
  }

  removeItem(product){
    
  }

  getItemsInCart(){
    const itemsInCart = JSON.parse(localStorage.getItem('Mycart'));
    return Object.values(itemsInCart.products);
  }

  getCartItemCount(): number{
    const itemsInCart = JSON.parse(localStorage.getItem('Mycart'));
    return Object.values(itemsInCart.products).length;
  }

  updateCartItemCount(){
    this.cartCount.next(this.getCartItemCount())
  }
}