import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { AceraSP } from '../models/acera-sp';

@Injectable({
  providedIn: 'root'
})
export class AceraSPService {
  API_URL = '//catastrocholoma.azurewebsites.net/api';

  constructor(private http: HttpClient) { }

  getAceraSPs() {
    return this.http.get(`${this.API_URL}/aceraSP`);
  }
  
  getAceraSP(tipo: string) {
    return this.http.get(`${this.API_URL}/aceraSP?tipo=${tipo}`)
  }

  saveAceraSP(aceraSP: AceraSP) {
    return this.http.post(`${this.API_URL}/aceraSP`, aceraSP);
  }

  modifyAceraSP(aceraSP: AceraSP) {
    return this.http.put(`${this.API_URL}/aceraSP?old=${aceraSP.tipo}`, aceraSP);
  }

  deleteAceraSP(tipo: string) {
    return this.http.delete(`${this.API_URL}/aceraSP?old=${tipo}`);
  }
}