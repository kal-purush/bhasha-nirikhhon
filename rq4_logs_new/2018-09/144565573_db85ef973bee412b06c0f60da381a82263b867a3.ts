import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Http, Headers } from '@angular/http';
import { map } from "rxjs/operators";

@Injectable({
  providedIn: 'root'
})
export class AuthService {
  authToken: any;
  user: any;
  url = "http://localhost:4000"

  constructor(private http: Http) { }

  /** register user in the backend and send back response message */
  registerUser(user) {
    let headers = new Headers();
    headers.append('Content-Type', 'application/json');
    return this.http.post(`${this.uri}/users/register`, user, { headers: headers })
      .pipe(map(res => res.json()))
  }
}