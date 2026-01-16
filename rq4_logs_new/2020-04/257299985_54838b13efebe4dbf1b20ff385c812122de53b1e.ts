import { environment } from '../../environments/environment';
import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';

@Injectable({
  providedIn: 'root',
})
export class HttpService {
  constructor(private http: HttpClient) {}

  getAll(type: string) {
    return this.http.get(`${environment.apiUrl}/${type}`);
  }
  getById(type: string, id: any) {
    return this.http.get(`${environment.apiUrl}/${type}/${id}`);
  }
  post(type: string, data: any) {
    return this.http.post(`${environment.apiUrl}/${type}`, data, {
      headers: new HttpHeaders().set('content-type', 'application/json'),
    });
  }
  update(type: string, data: any) {
    return this.http.put(`${environment.apiUrl}/${type}/${data.id}`, data, {
      headers: new HttpHeaders().set('content-type', 'application/json'),
    });
  }
  delete(type: string, id: any) {
    return this.http.delete(`${environment.apiUrl}/${type}/${id}`);
  }
}