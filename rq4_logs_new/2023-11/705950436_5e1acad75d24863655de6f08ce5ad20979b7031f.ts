import { Injectable } from '@angular/core';
import { ApiService } from './api.service';
import { BehaviorSubject } from 'rxjs';
import { UtilityService } from './utility.service';

@Injectable({
  providedIn: 'root'
})
export class CartService {
  cartProductList = new BehaviorSubject<any[]>([]);
  displayedColumns = ['productName','restaurantName','quantity','price', 'action']

  constructor(
    private _apiService: ApiService,
    private _utilityService: UtilityService,
  ) {}
  
  addToCart(productid: any) {
    let productList = this.cartProductList.getValue();
    productList.push(productid);
    localStorage.setItem("cart",JSON.stringify(productList));
    this.cartProductList.next(productList);
  }

  checkout(data: any){
    this._apiService.post(data, 'order/add').then((response: any) => {
      if (response && response.body.status === 'OK') {
          this._utilityService.successMessage(response.body.message, response.statusText);                
      } else {
          this._utilityService.errorMessage(response.body.message, response.statusText);
      };
  })
  }
}