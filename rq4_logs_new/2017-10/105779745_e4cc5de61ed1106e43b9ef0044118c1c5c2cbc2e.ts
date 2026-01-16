import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import 'rxjs/add/operator/map';

import { Product } from "../../models/Product";
import { Api } from '../api/api';

@Injectable()
export class ProductsProvider {

  constructor(public http: Http, public api: Api) {
    // console.log('Hello ProductsProvider Provider');
  }

  query(params?: any) {
    return this.api.get('events', params)
      .map(resp => resp.json());
  }

  add(product: Product) {

  }

  delete(product: Product) {

  }
}