import { Injectable } from '@angular/core';
import { PortConfigService } from './port-config.service';
import { HttpClient } from '@angular/common/http';

@Injectable({
  providedIn: 'root'
})
export class DeleteproductService {

  constructor(private httpClient :HttpClient, private portNumber : PortConfigService) {   
  }

deleteProduct(id:number){
  return this.httpClient.delete<void>('http://localhost:' + this.portNumber.getPort() + '/bikeShop/deleteProduct/'+id ,{ 
    
});
}
                                                 
}