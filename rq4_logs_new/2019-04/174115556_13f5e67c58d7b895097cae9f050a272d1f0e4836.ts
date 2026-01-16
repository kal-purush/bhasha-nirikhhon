import { Injectable } from '@angular/core';
import { HttpClientModule, HttpClient, HttpHeaders } from '@angular/common/http';
import {Observable} from 'rxjs';
import {ServiceUrls} from '../WebServices/ServiceUrls';
import { LocationBody } from '../SystemAdministration/organization/LocationBody'


@Injectable({
  providedIn: 'root'
})
export class LocationService {

    constructor(private httpClient: HttpClient) { }
    httpOptions = {
      headers: new HttpHeaders({
        'Content-Type': 'application/json'
      })
    };
    doLogin(locationBody: LocationBody): Observable<any> {
      return this.httpClient.post<any>(ServiceUrls.baseUrls + ServiceUrls.AddLocation, JSON.stringify(locationBody), this.httpOptions);
    }
    
  }
  
