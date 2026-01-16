import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import{Nacionalidad}from '../models/nacionalidad'

@Injectable({
  providedIn: 'root'
})
export class NacionalidadService {

  API_URL = '//catastrocholoma.azurewebsites.net/api';

  constructor(private http: HttpClient) { }

  getNacionalidad() {
    return this.http.get(`${this.API_URL}/nacionalidad`);
  }

  saveNacionalidad(nacionalidad: Nacionalidad) {
    return this.http.post(`${this.API_URL}/nacionalidad`, nacionalidad);
  }

  getNacionalidades(pais: string) {
    return this.http.get(`${this.API_URL}/nacionalidad/${pais}`)
  }

  modifyNacionalidad(nacionalidad: Nacionalidad) {
    return this.http.put(`${this.API_URL}/nacionalidad/${nacionalidad.pais}`, nacionalidad);
  }

  deleteNacionalidad(pais: string) {
    return this.http.delete(`${this.API_URL}/nacionalidad/${[pais]}`);
  }

}