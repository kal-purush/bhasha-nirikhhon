import { environment } from './../../../../../environment/environment';
import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root',
})
export class ProfileService {
  private baseUrl = environment.baseUrl;
  constructor(private readonly httpClient: HttpClient) {}

  getById(id: string) {
    return this.httpClient.get<any>(`${this.baseUrl}/employes/${id}`);
  }

  udpate(data: any) {
    return this.httpClient.put<any>(`${this.baseUrl}/employes`, data);
  }
}