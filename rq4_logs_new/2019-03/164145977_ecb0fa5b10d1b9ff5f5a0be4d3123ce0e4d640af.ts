import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders, HttpHandler } from '@angular/common/http';
import { Observer, Subscription, Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})

export class ApiService {

  constructor(private http: HttpClient, private headers: HttpHeaders) {
    this.headers = new HttpHeaders({
      'Content-Type': 'application/json'
    })
  }

  login(auth: { username?: string, password?: string, token?: string }): Promise<object> {
    if (auth.token) this.headers.set('Authorization', `Bearer ${auth.token}`);
    return this.post('login', auth);
  }

  get(resource: string): Promise<object>;
  get(resource: string, observer: Observer<any>): Subscription;
  get(resource: string, observer?: Observer<any>): any {
    const observable = this.http.get(`/api/${resource}`, {
      headers: this.headers
    });
    return observer ?
      observable.subscribe(observer)
      : new Promise((res, rej) => {
        observable.subscribe(res, rej);
      });
  }
  post(resource: string, body: object): Promise<object>;
  post(resource: string, body: object, observer: Observer<any>): Subscription;
  post(resource: string, body: object, observer?: Observer<any>): any {
    const observable = this.http.post(`/api/${resource}`, body, {
      headers: this.headers
    });
    return observer ?
      observable.subscribe(observer)
      : new Promise((res, rej) => {
        observable.subscribe(res, rej);
      });
  }
  put(resource: string, body: object): Promise<object>;
  put(resource: string, body: object, observer: Observer<any>): Subscription;
  put(resource: string, body: object, observer?: Observer<any>): any {
    const observable = this.http.post(`/api/${resource}`, body, {
      headers: this.headers
    });
    return observer ?
      observable.subscribe(observer)
      : new Promise((res, rej) => {
        observable.subscribe(res, rej);
      });
  }
}