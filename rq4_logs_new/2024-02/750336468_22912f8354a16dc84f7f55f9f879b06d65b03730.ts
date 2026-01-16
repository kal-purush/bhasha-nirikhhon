import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { environment } from 'src/environment/environment';

@Injectable({
  providedIn: 'root',
})
export class ClientProfilService {
  baseUrl = environment.baseUrl;
  constructor(private readonly httpClient: HttpClient) {}

  getProfile(id: string) {
    return this.httpClient.get<any>(`${this.baseUrl}/clients/${id}`);
  }

  updateProfile(id: string, data: any) {
    return this.httpClient.put<any>(`${this.baseUrl}/clients/${id}`, data);
  }
}