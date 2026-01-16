import { CommonModule } from '@angular/common';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { Product } from './product.model';
import { Cart } from './../app/store/cart.model';
import { Order } from './order.model';
import { HttpClient } from '@angular/common/http'
import { map } from 'rxjs/operators'
import { ExecFileOptionsWithStringEncoding } from 'child_process';

const PROTOCOL= 'http';
const PORT= 4210 ;

@Injectable()

export class RestDataSource {
    baseUrl: string;
    authToken: string;

    constructor(private http: HttpClient) {
        this.baseUrl= `${PROTOCOL}://${location.hostname}:${PORT}/`
    }

    getProduct(): Observable<Product[]> {
        return this.http.get<Product[]>(this.baseUrl + 'products');
    }

    saveOrders(order: Order) : Observable<Order> {
        return this.http.post<Order>(this.baseUrl + 'orders', order);
    }

    authenticate(user: string, pass: string): Observable<boolean> {
        return this.http.post<any>(this.baseUrl + 'login', 
        { name: user, password: pass}).pipe(map(response =>
            {
                this.authToken = response.success? response.token = null;
                return response.success;
            }));
    }
}