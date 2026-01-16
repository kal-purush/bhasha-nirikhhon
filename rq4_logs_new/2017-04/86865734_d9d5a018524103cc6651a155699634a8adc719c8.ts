import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import 'rxjs/add/operator/map';

import { Observable } from 'rxjs/Observable';
import { ApiEndpoints, ApiExtensions } from '../app/constants';
/*
  Generated class for the RatingsProvider provider.

  See https://angular.io/docs/ts/latest/guide/dependency-injection.html
  for more info on providers and Angular 2 DI.
*/
@Injectable()
export class RatingsProvider {

  constructor(public http: Http) {
    console.log('Hello RatingsProvider Provider');
  }

}