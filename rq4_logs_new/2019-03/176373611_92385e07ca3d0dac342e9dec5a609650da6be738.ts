import {Injectable} from '@angular/core'
import {HttpClient} from "@angular/common/http";
import {environment} from "../../environments/environment";

@Injectable()
export class CityDataService {

  constructor(private http: HttpClient) {
  }

  getCityDataByName(name: string, country: string = '') {
    if (country !== '') {
      return this.http.get('api.openweathermap.org/data/2.5/weather?q=' + name + ',' + country + '&APPID=' + environment.api_key)

    } else {
      return this.http.get('api.openweathermap.org/data/2.5/weather?q=' + name + '&APPID=' + environment.api_key)

    }
  }

  getCityDataById(id: number) {
    return this.http.get('api.openweathermap.org/data/2.5/weather?id=' + id + '&APPID=' + environment.api_key)
  }

  getCityForecastByName(name: string, country: string = '') {
    if (country !== '') {
      return this.http.get('api.openweathermap.org/data/2.5/forecast?q=' + name + ',' + country + '&APPID=' + environment.api_key)

    } else {
      return this.http.get('api.openweathermap.org/data/2.5/forecast?q=' + name + '&APPID=' + environment.api_key)

    }
  }

  getCityForecastById(id: number) {
    return this.http.get('api.openweathermap.org/data/2.5/forecast?id=' + id + '&APPID=' + environment.api_key)
  }

  getCitiesInArea(lat: number, lon: number, count: number) {
    return this.http.get('api.openweathermap.org/data/2.5/find?lat=' + lat + '&lon=' + lon + '&cnt=' + count + '&APPID=' + environment.api_key)
  }
}