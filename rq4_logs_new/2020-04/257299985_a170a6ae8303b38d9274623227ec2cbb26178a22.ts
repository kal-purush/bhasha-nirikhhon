import { environment } from './../../../environments/environment';
import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';

@Injectable({
  providedIn: 'root',
})
export class HttpService {
  private httpHeader = new HttpHeaders();
  constructor(private http: HttpClient) {
    this.httpHeader.append('Access-Control-Allow-Origin', '*');
    this.httpHeader.append('Access-Control-Allow-Credentials', 'true');
    this.httpHeader.append('content-type', 'application/json');
    this.httpHeader.append('data-type', 'jsonp');
    this.httpHeader.append('accepts', 'json');
  }

  getAll(type: string) {
    return this.http.get(`${environment.apiUrl}/${type}`);
  }
  getById(type: string, id: any) {
    return this.http.get(`${environment.apiUrl}/${type}/${id}`, {
      headers: this.httpHeader,
    });
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