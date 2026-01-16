import { Injectable } from '@angular/core';
import { Product } from './product.class';
import { products } from './product.mockData';

@Injectable()
export class ProductService {
  constructor() { }
  getProducts(): Promise<Product[]> {
    return Promise.resolve(products);
  } // stub

  // the following function is to simulate a slow connection, it's just for testing

  // getProductsSlowly(): Promise<Product[]> {
  //   return new Promise(resolve => {
  //     // Simulate server latency with 2 second delay
  //     setTimeout(() => resolve(this.getProducts()), 2000);
  //   });
  // }
}