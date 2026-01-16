import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { Form } from '../login/login.component';

@Injectable({
  providedIn: 'root'
})
export class ApiService {
  private URL = 'http://127.0.0.1:9090/api/';
  private LOGIN_URL = `${this.URL}auth\\login`;
  private VERIFICATION = `${this.URL}verification\\auth`;

  constructor(private http: HttpClient) { }

  getOptions() {
    let USER = JSON.parse(localStorage.getItem('USER'));
    if  (USER == null) {
      USER = {
        token: ''
      };
    }
    const httpHeaders = new HttpHeaders({
      'Content-Type' : 'application/json',
      Authorization : USER.token
    });
    const options = {
      headers: httpHeaders
    };
    return options;
  }

  verificationOfAuth(): Observable<any> {
    return this.http.get<any>(this.VERIFICATION, this.getOptions());
  }

  postLogin(form: Form): Observable<any> {
    return this.http.post<any>(this.LOGIN_URL, form);
  }
}