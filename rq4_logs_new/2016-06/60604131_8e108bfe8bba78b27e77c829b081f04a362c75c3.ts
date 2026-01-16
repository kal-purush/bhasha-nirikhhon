import {Http, Headers} from '@angular/http';
import { Injectable } from '@angular/core';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/toPromise';

const BASE_URL = 'http://home-accountant.herokuapp.com/api/v1/categories/';
const HEADER = { headers: new Headers({ 'Content-Type': 'application/json' }) };

export interface Category {
  id: number;
  name: string;
  types: string;
  path_img: string;
};

@Injectable()
export class CategoryService {

  constructor(private http: Http) {}

  loadAllCategories(){
    return this.http.get(BASE_URL)
      .map(res => res.json())
      .toPromise();
  }

  loadSpendCategory(){
    return this.http.get(BASE_URL+'types/spend')
      .map(res => res.json())
      .toPromise();
  }

  loadIncomeCategory(){
    return this.http.get(BASE_URL+'types/income')
      .map(res => res.json())
      .toPromise();
  }
}