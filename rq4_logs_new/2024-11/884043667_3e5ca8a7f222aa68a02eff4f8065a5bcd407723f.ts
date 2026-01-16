import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { iFeatureMaps } from '../models/iFeatureMap';
import { iDataPlace } from '../models/iDataPlace';

@Injectable({
  providedIn: 'root',
})
export class MapsService {
  private dataPreview: iDataPlace[] = [
    {
      latitud: -92.63844,
      longitud: 16.73744,
      id_place: 1,
    },
  ];

  private listPalcesMap: iFeatureMaps[] = [];

  getPlacesMap() {}

  getUbicationPlaces(): iFeatureMaps[] {
    this.dataPreview.forEach((dataMap) => {
        const placeMap: iFeatureMaps = {
            type: 'Feature',
            geometry: {
                type: 'Point',
                coordinates: [dataMap.latitud, dataMap.longitud]
            },
            properties: {
                popupContent: 'Centro de donación'
            }
        }
        this.listPalcesMap.push(placeMap);
    });
    return this.listPalcesMap;
  }
}