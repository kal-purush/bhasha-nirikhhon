import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { AddressesBackendService } from './addresses-backend-service';
import { Address } from '../models/address';

@Injectable()
export class HttpAddressesBackendService extends AddressesBackendService {

  private addAddressUrl: string = "api/addresses/addaddress";
  private getAllUrl: string = "api/addresses/getaddresses";
  private updateAddressUrl: string = "api/addresses/updateaddress";
  private getAddressUrl: string = "api/addresses/getaddress?addressId=";


  constructor(private http: HttpClient) {
    super();

  }
  addAddress(newAddress: Address) {
    const headers = new HttpHeaders().set('Content-Type', 'application/json');
    return this.http.post<Address>(this.addAddressUrl, JSON.stringify(newAddress), { headers })
  }
  updateAddress(newddress: Address) {
    const headers = new HttpHeaders().set('Content-Type', 'application/json');
    return this.http.post<Address>(this.updateAddressUrl, JSON.stringify(newddress), { headers });
  }
  getAll(): Observable<Address[]> {
    const headers = new HttpHeaders().set('Content-Type', 'application/json');
    return this.http.get<Address[]>(this.getAllUrl, { headers });
  }
  getAddress(id: number): Observable<Address> {
    const headers = new HttpHeaders().set('Content-Type', 'application/json');
    return this.http.get<Address>(this.getAddressUrl + id, { headers });
  }
}