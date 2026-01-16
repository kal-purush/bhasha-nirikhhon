import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { Order } from 'src/app/core/interfaces/order';
import { AuthenticationService } from './authentication.service';

@Injectable({
  providedIn: 'root',
})
export class OrderService {
  constructor(
    private http: HttpClient,
    private authService: AuthenticationService
  ) {}

  getOrders(): Observable<Order[]> {
    return this.http.get<Order[]>('http://localhost:4000/customer/orders', {
      headers: {
        'x-access-token': this.authService.accessToken,
      },
    });
  }
}