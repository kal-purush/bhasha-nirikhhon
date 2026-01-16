import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { Router } from '@angular/router';
import { window } from 'rxjs/internal/operators/window';

export interface User {
  email: string;
  password: string;
}
interface TokenResponse {
  token: string;
  name: string;
  role: string
}

@Injectable()
export class AuthenticationService {
  private token: string='';

  constructor(private http: HttpClient, private router: Router) {}

  private saveToken(token: string): void {
    sessionStorage.setItem('token', token);
    this.token = token;
  }

  private getToken(): string {
    if (!this.token) {
      this.token = sessionStorage.getItem('token');
    }
    return this.token;
  }

  public login(user: User): Observable<any> {
    let base;

      base = this.http.post('/api/login', user);
    

    const request = base.pipe(
      map((data: TokenResponse) => {
        if (data.token) {
          console.log(data);
          this.saveToken(data.token);
        }
        return data;
      })
    );

    return request;
  }

}