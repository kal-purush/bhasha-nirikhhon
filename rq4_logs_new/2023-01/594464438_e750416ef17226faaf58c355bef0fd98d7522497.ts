import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../environments/environment';

const API_BASE_URL = environment.apiURL;

@Injectable({
  providedIn: 'root'
})
export class HttpService {
  constructor(private http: HttpClient) { }

  get(url: string, params?: any) {
    return this.http.get(API_BASE_URL + url, { params });
  }

  post(url: string, data: any) {
    return this.http.post(API_BASE_URL + url, data);
  }

  put(url: string, data: any) {
    return this.http.put(API_BASE_URL + url, data);
  }

  delete(url: string, params?: any) {
    return this.http.delete(API_BASE_URL + url, { params });
  }

  patch(url: string, data: any) {
    return this.http.patch(API_BASE_URL + url, data);
  }
}