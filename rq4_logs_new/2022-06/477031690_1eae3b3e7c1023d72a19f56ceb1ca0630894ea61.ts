import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import * as L from 'leaflet'
import { MarkerService } from './marker.service';

@Injectable({
  providedIn: 'root'
})
export class MapServiceService {

  private apiKey: string = "&apiKey=d595e96962364b8aa09e1c5ef06d5286"

  constructor(private http: HttpClient, private markerService: MarkerService) { }

  // Obtener el lugar a partir de las coordenadas
  private async getPlaceByCoords(latitude: number, longitude: number) {
    var requestOptions = {
      method: 'GET'
    }
    return new Promise(resolve => {
      this.http.get(`https://api.geoapify.com/v1/geocode/reverse?lat=${latitude}&lon=${longitude}&apiKey=d595e96962364b8aa09e1c5ef06d5286`)
        .subscribe(data => {
          resolve(data)
        }, error => {
          console.log(error)
        })
    })
  }
  async getCityByCoords(latitude: number, longitude: number) {
    let city: string
    this.getPlaceByCoords(latitude, longitude).then((result) => {
      city = result['features'][0]['properties']['city']
    })
    return city
  }

  private async getCoordsByPlace(name: string) {
    var requestOptions = {
      method: 'GET',
    }

    return new Promise(resolve => {
      this.http.get(`https://api.geoapify.com/v1/geocode/search?text=${name}&apiKey=d595e96962364b8aa09e1c5ef06d5286`)
        .subscribe(data => {
          resolve(data)
        }, error => {
          console.log('error', error)
        })
    })
  }

  public async buscarCiudad(map: L.Map, name: string) {
    var requestOptions = {
      method: 'GET'
    }
    var coords = { lat: 0, lng: 0 }
    try {
      await fetch("https://api.geoapify.com/v1/geocode/search?text=" + name + "&filter=countrycode:es&apiKey=d595e96962364b8aa09e1c5ef06d5286", requestOptions)
        .then(response => response.json())
        .then(result => {
          coords = {
            lat: result['features'][0]['properties']['lat'],
            lng: result['features'][0]['properties']['lon']
          }
          this.markerService.putTmpMarker(new L.LatLng(coords.lat, coords.lng), map)
          map.flyTo(new L.LatLng(coords.lat, coords.lng), 10)
        })
    } catch (error:any) {
      this.moveStart(map)
    }
  }

  public moveStart(map: L.Map) {
    map.flyTo(new L.LatLng(40, -3), 6)
  }

}