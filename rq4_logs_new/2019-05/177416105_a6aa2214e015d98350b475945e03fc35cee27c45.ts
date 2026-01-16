import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Warehouse } from '../models/warehouse.model';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class WarehouseService {

  private warehouseUrl =  `${environment.apiBaseUrl + '/warehouse'}`;
  constructor(private http: HttpClient) { }

  getWarehouse (id: string) {
   
    const url = `${this.warehouseUrl}/${id}`;
    return this.http.get<Warehouse>(url).toPromise();

  }

  
  getWarehouses () {  
    
    return this.http.get<Warehouse[]>(this.warehouseUrl).toPromise();

  }
}