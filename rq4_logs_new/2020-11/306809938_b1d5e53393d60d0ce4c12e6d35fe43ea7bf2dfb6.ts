import { Injectable } from '@angular/core';
import {Router} from '@angular/router';
import {HttpClient} from '@angular/common/http';
import {Observable} from 'rxjs';
import {ParkingRe} from '../model/parkingRe';

@Injectable({
  providedIn: 'root'
})
export class ParkingReService {
  API_URL = 'http://localhost:8080';

  constructor(private router: Router, private httpClient: HttpClient) {
  }

  checkIn(license: string): Observable<ParkingRe> {
    return this.httpClient.get(this.API_URL + '/check-in/' + license);
  }

  checkOut(license: string): Observable<ParkingRe> {
    return this.httpClient.get(this.API_URL + '/check-out/' + license);
  }
}