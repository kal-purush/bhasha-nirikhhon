import { Injectable } from '@angular/core';
import { HttpHeaders, HttpClient } from '@angular/common/http';
import 'rxjs/add/operator/map';
import { Weather } from '../../shared/models/weather.model';

const httpOptions = {
  headers: new HttpHeaders({ 'Content-Type': 'application/json' })
};

@Injectable()
export class WeatherInformationService {

  constructor(private http: HttpClient) { }

  getCityTemperature() {
    return this.http.get(`http://api.openweathermap.org/data/2.5/weather?q=London&appid=164b4792bbb9d92dde20e42d2a6cebac`);
  }

}