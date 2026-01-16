import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders, HttpResponse } from '@angular/common/http';
import { environment } from '../../environments/environment';
import {Observable} from 'rxjs';

@Injectable({
  providedIn: 'root'
})

export class AuthService {
  URL = environment.FLIGHTS_API;

  constructor(private http: HttpClient) {}

  postUser(payload: any): Observable<any> {
    const url = this.URL + '/auth/sign-up';
    return this.http.post(url, payload);
  }

  confirmUser(token: string): Observable<any> {
    const url = this.URL + '/auth/confirm/' + token;
    return this.http.get(url);
  }

  loginUser(payload: any): Observable<any> {
    const url = this.URL + '/auth/login';
    console.log({ URL });
    console.log('Payload: ' + payload.password);
    return this.http.post(url, payload);
  }

  logout(): void {
    localStorage.removeItem('token');
 }
}
