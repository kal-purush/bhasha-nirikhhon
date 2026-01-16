import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { PortConfigService } from './port-config.service';

@Injectable({
  providedIn: 'root'
})
export class SaleService {

  constructor( private httpClient: HttpClient, private portNumber: PortConfigService ) { }

  createSale(discount: number, productID: number) {
    return this.httpClient.post<boolean>('http://localhost:' + this.portNumber.getPort() + '/bikeShop/createSale', {
      'discount': discount,
      'productID': productID,
    });
  }
  deleteSale(productID: number) {
    return this.httpClient.post<boolean>('http://localhost:' + this.portNumber.getPort() + '/bikeShop/deleteSale', {
      'productID': productID
    })
  }
}