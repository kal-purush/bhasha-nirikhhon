import { Injectable } from '@angular/core';
import {Router} from '@angular/router';
import {Observable} from 'rxjs';
import {Car} from '../car/car';
import {HttpClient} from '@angular/common/http';

@Injectable({
  providedIn: 'root'
})
export class CarService {
  API_URL = 'http://localhost:8080';
  proxyUrl = 'https://cors-anywhere.herokuapp.com/';
  PLATE_API = 'https://api.platerecognizer.com/v1/plate-reader/';

  constructor(private router: Router, private httpClient: HttpClient) { }

  getCarByLicense(license: number): Observable<Car> {
    return this.httpClient.get<Car>(`${this.API_URL}/get-car/${license}`);
  }

  saveCar(car: Car): Observable<any>{
    return this.httpClient.post<any>(`${this.API_URL}/add-car`, car);
  }

  getLicense(image: FormData): Observable<any>{
    // image.append('Access-Control-Allow-Origin', '*');
    // image.append('Access-Control-Allow-Methods', 'GET, POST, PATCH, PUT, DELETE, OPTIONS');
    // image.append('Access-Control-Allow-Headers', 'Origin, Content-Type, X-Auth-Token');
    return this.httpClient.post<any>(this.proxyUrl + this.PLATE_API,
      image, {headers: {
          Authorization: 'Token b254bd5bb420657fdf9b07b597db61a8dbaee6e8'
        }});
  }
}