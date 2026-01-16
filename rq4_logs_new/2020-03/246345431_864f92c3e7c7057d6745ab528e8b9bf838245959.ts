import { Component, OnInit, OnDestroy } from "@angular/core";
import { Subscription } from 'rxjs';

import { Order } from "../order.model";
import { OrdersService } from "../orders.service";

@Component({
  selector: "app-order-list",
  templateUrl: "./order-list.component.html",
  styleUrls: ["./order-list.component.css"]
})
export class OrderListComponent implements OnInit {
  orders: Order[] = [];
  isLoading = false;
  private ordersSub: Subscription;

  constructor(public ordersService: OrdersService) {}

  ngOnInit() {
    this.isLoading = true;
    this.ordersService.getOrders();
    this.ordersSub = this.ordersService.getOrderUpdateListener()
      .subscribe((orders: Order[]) => {
        this.isLoading = false;
        this.orders = orders;
      });
  }



}