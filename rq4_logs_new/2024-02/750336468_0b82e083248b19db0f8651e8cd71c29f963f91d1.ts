import { AuthData, LoginDto } from './../dtos/login.dto';
import { HttpClient } from '@angular/common/http';
import { environment } from './../../../../../environment/environment';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root',
})
export class LoginService {
  private readonly base_url = environment.baseUrl;
  constructor(private readonly http: HttpClient) {}

  onLogIn(data: LoginDto) {
    return this.http.post<AuthData>(`${this.base_url}/credentials/login`, data);
  }
}