import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root',
})
export class CheckJWTService {
  constructor(private _http: HttpClient) {}
  private token = localStorage.getItem('token');
  private URL = 'http://localhost:3000/check/';

  checkToken(): Observable<any> {
    const headers = new HttpHeaders({
      Authorization: `Bearer ${this.token}`,
    });
    return this._http.get(this.URL, { headers });
  }
}