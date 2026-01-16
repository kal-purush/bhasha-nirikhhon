import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { iAccess } from '../models/iAccess';

@Injectable({
  providedIn: 'root',
})
export class DoneesService {
  private URL_BASE: string = 'http://localhost:5000/donees';

  constructor(private _http: HttpClient) {}

  login(credentials: iAccess): Observable<any> {
    const headers = new HttpHeaders({
      'Content-Type': 'application/json',
    });
    return this._http.post(`${this.URL_BASE}/login`, credentials, { headers });
  }
  
}