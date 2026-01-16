import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { iAddress } from '../models/iAddress';

@Injectable({
  providedIn: 'root',
})
export class AddressService {
  private URL_BASE: string =
    'http://localhost:5000/address/';

  constructor(private _http: HttpClient) {}

  getAddress(postalCode: string): Observable<iAddress> {
    return this._http.get<iAddress>(`${this.URL_BASE}${postalCode}`);
  }
}