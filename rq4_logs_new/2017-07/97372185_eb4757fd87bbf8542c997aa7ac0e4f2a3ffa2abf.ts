import {Injectable} from '@angular/core';

interface Point {
  lat: number; lon: number;
}

@Injectable()
export class ESPMapService {

  private nativeMap;
  private agmMap;

  constructor() {
  }

  registerMaps(nativeMap, agmMap) {
    this.nativeMap = nativeMap;
    this.agmMap = agmMap;
  }

  setCenter(lat: number, lon: number) {
    this.nativeMap.setCenter({'lat': lat, 'lng': lon});
  }

  setZoom(zoomLevel: number) {
    this.nativeMap.setZoom(zoomLevel);
  }

  /**
   * Get points (any object), which has 'lat'  and 'lon' property
   * @param Points
   */
  calculateCentroid(Points: Point[]) {
    let outputlat = 0;
    let outputlon = 0;

    Points.forEach((p) => {
      outputlat += p.lat;
      outputlon += p.lon;
    });

    outputlat = outputlat / Points.length;
    outputlon = outputlon / Points.length;

    return {'lat': outputlat, 'lon': outputlon};
  }
}