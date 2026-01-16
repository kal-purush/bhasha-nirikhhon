import { Injectable } from '@angular/core';
import { Customer } from './customer'
import { Http, Headers, RequestOptions } from '@angular/http';
import {Observable} from 'rxjs/Rx';

import 'rxjs/add/operator/map';
import 'rxjs/add/operator/toPromise';
@Injectable()
export class DataService {

  result:any;

  constructor(private _http: Http) { }

  // getUsers() {
  //   return this._http.get("/api/users")
  //     .map(result => this.result = result.json().data);
  // }
  postCustomers(post: Customer){
    console.log(Customer);
    let headers = new Headers({ 'Content-Type': 'application/json'});
    let options = new RequestOptions({ headers: headers });
    return this._http.post("/api/addCustomer",JSON.stringify(post))
    .map(result => this.result = result.json().data);
  }

}