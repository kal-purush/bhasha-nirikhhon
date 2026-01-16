import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { DetallesAdicionales } from '../models/detalles-adicionales';

@Injectable({
  providedIn: 'root'
})
export class DetallesAdicionalesService {

  API_URL = '//catastrocholomaapi.azurewebsites.net/api';

  constructor(private http: HttpClient) { }

  getDetallesAdicionales() {
    return this.http.get(`${this.API_URL}/DetallesAdicionales`);
  }

  saveDetallesAdicionales(DetallesAdicionales: DetallesAdicionales){
    return this.http.post(`${this.API_URL}/DetallesAdicionales`, DetallesAdicionales);
  }

  getDetallesAdicional(id: string) {
    return this.http.get(`${this.API_URL}/DetallesAdicionales/${id}`)
  }

  modifyDetallesAdicionales(DetallesAdicionales: DetallesAdicionales) {
    return this.http.put(`${this.API_URL}/DetallesAdicionales/${DetallesAdicionales.idClaveCatastral}`, DetallesAdicionales);
  }

  deleteDetallesAdicionales(id: string) {
    return this.http.delete(`${this.API_URL}/DetallesAdicionales/${id}`);
  }
}