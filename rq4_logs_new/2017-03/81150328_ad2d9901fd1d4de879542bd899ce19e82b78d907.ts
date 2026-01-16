import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import 'rxjs/add/operator/map';
import {GeolocationService} from "./geolocation-service";

/*
  Generated class for the SettingsService provider.

  See https://angular.io/docs/ts/latest/guide/dependency-injection.html
  for more info on providers and Angular 2 DI.
*/
@Injectable()
export class SettingsService {

  // Not celsius = fahrenheit
  celsius: boolean;
  gender: string;
  location: Position;

  constructor(public http: Http) {
    this.celsius = false;
    this.gender = "female";
    let geolocation = new GeolocationService();
    geolocation.load()
      .then(data => this.location = data)
  }

  setCelsius() {
    this.celsius = true;
  }

  setFahrenheit() {
    this.celsius = false;
  }

  setMale() {
    this.gender = "male";
  }

  setNeutral() {
    this.gender = "neutral";
  }

  setLocation(pos: Position) {
    this.location = pos;
  }

}