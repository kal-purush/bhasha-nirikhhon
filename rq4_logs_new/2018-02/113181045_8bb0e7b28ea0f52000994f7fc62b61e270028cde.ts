import { Injectable } from '@angular/core';
import { HttpClient, HttpParams, } from '@angular/common/http';

@Injectable()
export class LocationService {

  constructor(private http: HttpClient) { }

  public fetchClosest() {
     if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition((position) => {
           console.log(position);
           this.http.get('stations', {
              params: new HttpParams ({
                 fromObject: {
                    longtitude: position.coords.longitude.toString(),
                    latitude: position.coords.latitude.toString(),
                    accuracy: position.coords.accuracy.toString(),
                 }
              })
           }).subscribe((result: any): void => {
              console.log(result);
           });

        });
     } else {
        console.log('Geolocation is not supported by this browser.');
     }
  }

}