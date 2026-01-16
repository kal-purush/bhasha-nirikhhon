import {Component, OnInit, ViewContainerRef} from '@angular/core';

import {AuthenticationService} from '../../shared/services/authentication.service';
import {ChangeOrder} from '../models/changeOrder.model';
import {Order} from '../models/order.model';
import {ToastsManager} from 'ng2-toastr/src/toast-manager';
import {UsersService} from '../users.service';

@Component({
  selector: 'app-view-orders',
  templateUrl: './view-orders.component.html',
  styleUrls: ['./view-orders.component.css']
})
export class ViewOrdersComponent implements OnInit {

  public order: ChangeOrder;
  public id: number;
  public finished: boolean;
  public sortBy: string[];
  public sortFilterValue: string;
  public filterObj: {};
  public orders: any;
  public ordersValue: Order[];
  public statuses: boolean[];
  public response: any;
  public isLoggedIn: boolean;
  public isAdminLoggedIn: boolean;


  constructor(private usersService: UsersService, public toastr: ToastsManager, public vcr: ViewContainerRef, private authenticationService: AuthenticationService) {
    this.toastr.setRootViewContainerRef(vcr);
    this.statuses = [false, true];
    this.sortBy = ['all', 'number', 'date', 'finished', 'pending'];
    this.isLoggedIn = this.authenticationService.isLoggedIn;
    this.isAdminLoggedIn = this.authenticationService.isAdminLoggedIn;
  }

  public sortOrdersBy(sortFilterValue) {

  const obj = Object.create(this.ordersValue);
    if (sortFilterValue === 'finished') {
      this.orders = obj.filter((order) => {
         return order.Finished === true;
      });
    }
    if (sortFilterValue === 'pending') {
      return this.orders = obj.filter((order) => {
        return order.Finished === false;
      });
    }

    if (sortFilterValue === 'number') {
      return this.orders = obj.sort((a, b) => {
        return a.Id - b.Id;
        }
      );
    }

    if (sortFilterValue === 'all') {
      return this.orders = obj;
    }

    if (sortFilterValue === 'date') {
      return this.orders = obj.sort((a, b) => {
        return new Date(a.CreatedOn).getTime() - new Date(b.CreatedOn).getTime();
        }
      );
    }
  }

  public selectNewStatus(finished: boolean, id: number) {
    this.finished = finished;
    this.id = id;
    this.usersService.changeOrderStatus(this.finished, this.id)
      .subscribe(
        (response: Response) => {
          this.response = response;
          this.toastr.success('Status changed!', 'SUCCESS!');
        },
        (error: Error) => {
          this.toastr.error('Failed to change status!', 'ERROR!');
        }
    );
    this.ngOnInit();
  }
  ngOnInit() {
    if (this.isLoggedIn && this.isAdminLoggedIn) {
      this.usersService.maganeOrders()
      .subscribe(
        orders => {
          this.orders = orders;
          this.ordersValue = orders;
        });
    }
    if (this.isLoggedIn && !this.isAdminLoggedIn) {
      this.usersService.getUserOrders()
      .subscribe(
        orders => {
          this.orders = orders;
          this.ordersValue = orders;
        });
    }
  }

}