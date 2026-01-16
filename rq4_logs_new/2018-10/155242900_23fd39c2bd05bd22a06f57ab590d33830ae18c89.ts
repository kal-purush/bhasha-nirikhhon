import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from "@angular/common/http";
import { Observable, of } from "rxjs";
import { catchError } from "rxjs/operators";
import { Support } from "./support"

@Injectable({
  providedIn: 'root'
})
export class SupportService {

  constructor(private http: HttpClient) { }

  httpOptions = {
    headers: new HttpHeaders ({"Content-Type": "application/json"})
  };

  private supportUrl = " https://team-buh-gok-server.herokuapp.com/api/ticket";

  private handleError<T>(operation = "operation", result?: T) {
    return(error: any): Observable<T> => {
      console.error(error);
      return of(result as T);
    };
  }

  support(form: Support): Observable<any> {
    return this.http
    .post<Support>(`${this.supportUrl}`, form,this.httpOptions)
    .pipe(catchError(this.handleError<Support>("suppor")))
  }

}