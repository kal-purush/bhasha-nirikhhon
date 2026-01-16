import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { tap } from 'rxjs/operators';
import { UserAuthResponse, UserLogin } from '../types';

@Injectable({providedIn: 'root'})
export class UserAuthService {

  AUTH_URL = 'http://52.57.253.240:3000/api/auth';

  constructor(private http: HttpClient) { }

  get token(): string {
    return '';
  }

  login(userLogin: UserLogin): Observable<UserAuthResponse> {
    return this.http.post(this.AUTH_URL, userLogin).pipe(
      tap(this.setToken)
    );
  }

  logout(): void {

  }

  isAuthenticated(): boolean {
    return !!this.token;
  }

  private setToken(response: UserAuthResponse): void {
    console.log(response);
  }
}