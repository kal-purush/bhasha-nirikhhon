import { Injectable } from '@angular/core';
import { Headers, Http } from '@angular/http';

import 'rxjs/add/operator/toPromise';

import { Item } from './item';
import { cities } from './mock-cities';

@Injectable()
export class CitiesService {
  getCities(): Promise<Item[]> {
    return Promise.resolve(cities);
  }

  // private citiesUrl = 'api/cities';  // URL to web api
  //
  // constructor(private http: Http) { }
  //
  // getCities(): Promise<Item[]> {
  //   return this.http.get(this.citiesUrl)
  //     .toPromise()
  //     .then(response => response.json().data as Item[])
  //     .catch(this.handleError);
  // }
  //
  // private handleError(error: any): Promise<any> {
  //   console.error('An error occurred', error); // for demo purposes only
  //   return Promise.reject(error.message || error);
  // }

}