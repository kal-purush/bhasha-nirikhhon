import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import { Item } from '../../models';
import { Storage } from '@ionic/storage';
import { Observable } from "rxjs/Observable";

@Injectable()
export class CartService {
	public cartItems: Map<String, number>;

	/**
	 *
	 * Creates an instance of CartCallService.
	 * @param {Http} http 
	 * @param {Storage} storage 
	 * 
	 * @memberOf CartCallService
	 */
	constructor(private http: Http, public storage: Storage) {
		this.cartItems = new Map<String, number>();
	}

	/**
	 * Adds and item to the cart
	 * @param cartItem 
	 */
	public addToCart(cartItem: Object): void {
		this.storage.get('CartItem').then(value => {

			value == null ? this.cartItems = new Map<String, number>() : this.cartItems = value;
			console.log(cartItem);

			this.cartItems.set(cartItem['id'], 1)
			this.storage.set('CartItem', this.cartItems)
		})
	}


	public getCartItems(): Array<Object> {
		let cartItems = [];
		this.storage.get('cartItem').then(value => {
			value == null ? this.cartItems = new Map<String, number>() : this.cartItems = value;
			this.cartItems.forEach((value, key) => {
				this.http.get('https://keanubackend.herokuapp.com/item/id/' + key).map(res => res.json)
					.subscribe(
					data => {
						cartItems.push({
							'item': data,
							'quantity': value
						})
					},
					err => console.log(err),
					() => console.log('done getting ' + key)
					)
			})
			return cartItems;
		})




	}
}