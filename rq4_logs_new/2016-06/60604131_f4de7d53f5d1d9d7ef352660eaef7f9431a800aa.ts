import {Http, Headers} from '@angular/http';
import { Injectable } from '@angular/core';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/toPromise';

const BASE_URL = 'https://api.privatbank.ua/p24api/pubinfo?json&exchange';
const HEADER = { headers: new Headers({ 'Content-Type': 'application/json' }) };

export interface Nbu {
  ccy: string;
  base_ccy: string;
  buy: string;
  sale: string;
};

@Injectable()
export class NbuService {

  constructor(private http: Http) {}

  loadNbu() {
    return this.http.get(BASE_URL)
      .map(res => res.json())
      .toPromise();
  }
}