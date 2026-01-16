import { Injectable } from '@angular/core';
import { Coordinates } from "../map/map.component";

@Injectable({
  providedIn: 'root'
})
export class GeolocationService {

  getLocation(): Promise<Coordinates> {
    return new Promise((resolve, reject) => {
      navigator.geolocation.getCurrentPosition(
          position => {
            resolve({
              lat: position.coords.latitude,
              lon: position.coords.longitude
            });
          },
          () => reject('Could not get your location')
      );
    });
  }

}