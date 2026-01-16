import { Injectable } from '@angular/core';
import { ApiService } from '../Api/api.service';
import { Http, Headers, Response } from '@angular/http';
import { Observable } from 'rxjs/Rx';
import 'rxjs/add/operator/map';
import { User } from '../../Classes/user';
@Injectable()
export class AuthenticationService {
  public token: string;

  constructor(private http: Http) {
    let currentUser = JSON.parse(localStorage.getItem('currentUser'));
    this.token = currentUser && currentUser.token;
  }

  login(email: string, password: string): Observable<boolean> {
    return this.http.post('http://localhost:3000/auth/login', { email: email, password: password })
      .map((response: any) => {
        const respons = JSON.parse(response._body);

        if (!respons.hasError) {

          this.token = respons.data.token;
          localStorage.setItem('currentUser', JSON.stringify({ user: respons.data.user, token: respons.data.token }));

          return true;
        } else {
          return false;
        }
      });
  }

  logout(): void {
    this.token = null;
    localStorage.removeItem('currentUser');
  }
}