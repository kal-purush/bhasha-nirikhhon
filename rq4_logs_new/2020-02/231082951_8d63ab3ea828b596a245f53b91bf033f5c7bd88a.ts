import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { catchError, map, tap } from 'rxjs/operators';
import { Observable, of } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class LogService {
  private serverURL = 'http://localhost:3000/';
  private historyURL = this.serverURL + 'history'
  private httpOptions = {
    headers: new HttpHeaders({
      'Content-Type': 'application/json',
      'Authorization': ''
    })
  };

    constructor(private http: HttpClient) { }

  loadLastHistory(num: number) {
      this.httpOptions.headers = this.httpOptions.headers.set('Authorization', localStorage.getItem("token"));
      return this.http.get<any>(this.historyURL + "/" + num, this.httpOptions).pipe(
        catchError((error) => { return of(undefined); })
      );
    }
  }