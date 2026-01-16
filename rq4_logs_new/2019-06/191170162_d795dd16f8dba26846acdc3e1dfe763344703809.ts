import { PortConfigService } from './port-config.service';
import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class MakePurchaseService {

  constructor(private httpClient: HttpClient, private portNumber: PortConfigService) { }

  makePuchase(product: object) {
    console.log('Sending product:' + product);
    return this.httpClient.post<boolean>('http://localhost:' + this.portNumber.getPort() + '/bikeShop/products/makePurchase', {
      //'product': product
      'id': product['id'],
      'name': product['name'],
      'upc': product['upc'],
      'price': product['price'],
      'description': product['description'],
      'stock': product['stock'],
      'image': product['image'],
      'type_id': product['type_id'],
    });
  }
}