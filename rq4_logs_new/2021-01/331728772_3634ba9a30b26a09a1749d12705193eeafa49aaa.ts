import { Injectable } from '@angular/core';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import { map } from 'rxjs/operators';
import { API_URL} from '../../app.apiurl';

export const TOKEN = 'token';
export const AUTHENTICATED_USER = 'authenticatedUser';

@Injectable({
  providedIn: 'root'
})
export class AuthenticationService {

  constructor(
    private http: HttpClient
  ) { }

  getAuthenticatedUser = () => {
    return sessionStorage.getItem(AUTHENTICATED_USER);
  }

  executeAuthService = (username: string, password: string) => this.http.post<any>(
    `${API_URL}/authenticate`, {
      username,
      password
    }
  ).pipe(
    map(
      response => {
        sessionStorage.setItem(AUTHENTICATED_USER, username);
        sessionStorage.setItem(TOKEN, `Bearer ${response.token}`);
      }
    )
  )

  getAuthenticatedToken = () => {
    // @ts-ignore
    if (this.getAuthenticatedUser()) {
      return sessionStorage.getItem(TOKEN);
      const authHeader = {
        headers: new HttpHeaders({
          Authorization: 'Bearer ' + TOKEN,
        })
      };
      return this.http.get(`${API_URL}/myaccount/all/:id`);

    }
  }

  isUserLoggedIn = () => {
    const user = sessionStorage.getItem(AUTHENTICATED_USER);
    return !(user === null);
  }

  logout = () => {
    sessionStorage.removeItem(AUTHENTICATED_USER);
    sessionStorage.removeItem(TOKEN);
  }
}