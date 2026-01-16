import { Http } from '@angular/http';
import { Injectable } from '@angular/core';
import { Purchase } from './purchase';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';

@Injectable()
export class PurchaseService {

  constructor(private http: Http) { }

  savePurchase(purchase: Purchase): Observable<Purchase> {
    return this.http.post(``, JSON.stringify(purchase))
      .map(result => result.json())
      .catch(error => Observable.throw(error))
  }

}