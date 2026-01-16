import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { TipoEmpresa } from '../models/tipo-empresa';

@Injectable({
  providedIn: 'root'
})
export class TipoEmpresaService {
  API_URL = '//catastrocholoma.azurewebsites.net/api';

  constructor(private http: HttpClient) { }

  getTipoEmpresas() {
    return this.http.get(`${this.API_URL}/tipoEmpresa`);
  }
  
  getTipoEmpresa(empresa: string) {
    return this.http.get(`${this.API_URL}/tipoEmpresa?empresa={empresa}`)
  }

  saveTipoEmpresa(tipoEmpresa: TipoEmpresa) {
    return this.http.post(`${this.API_URL}/tipoEmpresa`, tipoEmpresa);
  }

  modifyTipoEmpresa(tipoEmpresa: TipoEmpresa) {
    return this.http.put(`${this.API_URL}/tipoEmpresa?old={tipoEmpresa.empresa}`, tipoEmpresa);
  }

  deleteTipoEmpresa(empresa: string) {
    return this.http.delete(`${this.API_URL}/tipoEmpresa?old={empresa}`);
  }
}