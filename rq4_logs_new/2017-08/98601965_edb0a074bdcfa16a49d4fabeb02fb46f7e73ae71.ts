import { Component, OnInit } from '@angular/core';
import {CartService} from '../../../service/cart.service';

@Component({
  selector: 'app-button-cart',
  templateUrl: './button-cart.component.html',
  styleUrls: ['./button-cart.component.css']
})
export class ButtonCartComponent implements OnInit {
  total: number;
  cart: any;
  constructor(private cartService: CartService) {
    this.cart = this.cartService;
  }
  ngOnInit() {
  }
  confirmOrder() {
    this.cartService.removeCart();
  }
}