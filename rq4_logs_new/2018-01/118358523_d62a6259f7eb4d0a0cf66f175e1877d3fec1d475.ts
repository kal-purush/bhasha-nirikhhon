import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs/Observable';
import { City } from './../models/city';
@Injectable()
export class LocationClientService {

  private baseURL = 'https://project-9-20-187720.appspot.com/';
  constructor( private http: HttpClient ) { }

  public getAllCities(): Observable<City[]> {
    const url = this.baseURL + 'cities';
    return this.http.get(url).map( res => {
      return res['Cities'].map( item => {
        return new City(
          item.city_name,
          item.city_id
        );
      });
    });
  }

}