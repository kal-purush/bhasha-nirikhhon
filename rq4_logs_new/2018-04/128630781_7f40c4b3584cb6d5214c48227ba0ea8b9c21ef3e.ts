import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

/*
  Generated class for the GeolocateProvider provider.

  See https://angular.io/guide/dependency-injection for more info on providers
  and Angular DI.
*/
@Injectable()
export class GeolocateProvider {

  constructor(public http: HttpClient) {
    console.log('Hello GeolocateProvider Provider');
  }

  googleAPIKey : string = "&key=AIzaSyDg5AKBNjMvoBBlLgXpy-dLxLAcVJYpOq8";
  convertLatLongUrl : string = "https://maps.googleapis.com/maps/api/geocode/json?latlng=";
  convertAddressUrl : string = "https://maps.googleapis.com/maps/api/geocode/json?address=";

  convertLatLong(lat, long) {
    var url = this.convertLatLongUrl + lat + "," + long + this.googleAPIKey;
    console.log("convertLatLng Url : ", url);
    return this.http.get(url);
  }

  convertAddress(address) {
    var url = this.convertAddressUrl + address + this.googleAPIKey;
    console.log("convertLatLng Url : ", url);
    return this.http.get(url);
  }
}