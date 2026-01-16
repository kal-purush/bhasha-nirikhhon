import { Injectable } from '@angular/core';
import { Http, Response, Headers, RequestOptions } from '@angular/http';
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';

import { PRODUCTS } from '../../../../assets/mock-data/product-data';

import 'rxjs/add/operator/catch';
import 'rxjs/add/operator/map';

@Injectable()
export class ProductService {

  public cart;
  public cart$: Subject<any>;

  constructor() {
    this.cart = [];
    this.cart$ = new Subject();
  }

  addToCart(product) {
    this.cart = [...this.cart, product]
    this.cart$.next(product)
  }

  // Returns an observable for the cart
  subcribeCart() {
    return Promise.resolve(this.cart$)
  }

  // Returns an array of objects of the items in the cart
  getCart() {
    return Promise.resolve(this.cart);
  }

  getProducts() {
    return Promise.resolve(PRODUCTS)
  }

  getProduct(id) {
    return this.getProducts()
      .then(products => products.find(product => product.id === id))
  }
}