import {Injectable} from '@angular/core'
import {HttpClient} from "@angular/common/http";
import {environment} from "../../environments/environment";
import {Observable, of} from "rxjs";
import {catchError} from "rxjs/operators";

@Injectable()
export class CityDataService {

  constructor(private http: HttpClient) {
  }

  private handleError<T>(operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(error);
      return of(result as T)
    }
  }

  getCityDataByName(name: string, country: string = '') {
    if (country !== '') {
      return this.http.get('https://api.openweathermap.org/data/2.5/weather?q=' + name + ',' + country + '&units=imperial' + '&APPID=' + environment.api_key)
        .pipe(catchError(this.handleError('getCityDataByName', [])))

    } else {
      return this.http.get('https://api.openweathermap.org/data/2.5/weather?q=' + name + '&units=imperial' + '&APPID=' + environment.api_key)
        .pipe(catchError(this.handleError('getCityDataByName', [])))
    }
  }

  getCityDataById(id: number) {
    return this.http.get('https://api.openweathermap.org/data/2.5/weather?id=' + id + '&units=imperial' + '&APPID=' + environment.api_key)
      .pipe(catchError(this.handleError('getCityDataById', [])))
  }

  getCityForecastByName(name: string, country: string = '') {
    if (country !== '') {
      return this.http.get('https://api.openweathermap.org/data/2.5/forecast?q=' + name + ',' + country + '&units=imperial' + '&APPID=' + environment.api_key)
        .pipe(catchError(this.handleError('getCityForecastByName', [])))

    } else {
      return this.http.get('https://api.openweathermap.org/data/2.5/forecast?q=' + name + '&units=imperial' + '&APPID=' + environment.api_key)
        .pipe(catchError(this.handleError('getCityForecastByName', [])))

    }
  }

  getCityForecastById(id: number) {
    return this.http.get('https://api.openweathermap.org/data/2.5/forecast?id=' + id + '&units=imperial' + '&APPID=' + environment.api_key)
      .pipe(catchError(this.handleError('getCityForecastById', [])))
  }

  getCitiesInArea(lat: number, lon: number, count: number) {
    return this.http.get('https://api.openweathermap.org/data/2.5/find?lat=' + lat + '&lon=' + lon + '&cnt=' + count + '&units=imperial' + '&APPID=' + environment.api_key)
      .pipe(catchError(this.handleError('getCitiesInArea', [])))
  }
}