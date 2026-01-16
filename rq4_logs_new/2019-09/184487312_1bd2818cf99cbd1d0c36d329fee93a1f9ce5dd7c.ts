import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import{ClaseDominio}from '../models/clase-dominio'
@Injectable({
  providedIn: 'root'
})
export class ClaseDominioService {

  
  API_URL = '//catastrocholoma.azurewebsites.net/api';

  constructor(private http: HttpClient) { }

  getClasesDominios() {
    return this.http.get(`${this.API_URL}/claseDominio`);
  }

  saveClaseDominio(clasedominio: ClaseDominio) {
    return this.http.post(`${this.API_URL}/claseDominio`, clasedominio);
  }

  getClaseDominio(tipo: string) {
    return this.http.get(`${this.API_URL}/claseDominio?tipo=${tipo}`)
  }

  modifyClaseDominio(clasedominio: ClaseDominio) {
    return this.http.put(`${this.API_URL}/claseDominio?old=${clasedominio.tipo}`, clasedominio);
  }

  deleteClaseDominio(tipo: string) {
    return this.http.delete(`${this.API_URL}/claseDominio?old=${tipo}`);
  }
}