import { Injectable, OnDestroy } from '@angular/core';
import { HttpClient, HttpHeaders, HttpParams } from '@angular/common/http';
import { Observable, Subscription, throwError } from 'rxjs';
import { catchError } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class ApiService implements OnDestroy {
 
  private subscription: Subscription;
  private urlApi : string = 'http://localhost:3000';

  constructor(private http: HttpClient) { }

  private httpOptions = {
    headers: new HttpHeaders({
      'Content-Type': 'application/json; charset=utf-8'
    })
  };

  private formatErrors(error: any){
    return throwError(error);
  }

  /**
   * @param path
   * @param params
   */
  get<T>(path: string, params?: HttpParams): Observable<T> {
    const httpOptions = { headers : this.httpOptions.headers, params : params};
    return this.http.get<T>(this.urlApi + path, httpOptions).pipe(catchError(this.formatErrors));
  }

  /**
   * 
   * @param path 
   */
  getUnhandled<T>(path: string): Observable<T> {
    const httpOptions = { headers : this.httpOptions.headers};
    return this.http.get<T>(this.urlApi + path);  
  }

  /**
   * 
   * @param path 
   * @param params 
   */
  put<T>(path: string, params: Object = {}): Observable<T> {
    return this.http.put<T>(this.urlApi + path, params, this.httpOptions).pipe(catchError(this.formatErrors));
  }

  /**
   * 
   * @param path 
   * @param params 
   */
  post<T>(path: string, params: Object = {}): Observable<T> {
    return this.http.post<T>(this.urlApi + path, params).pipe(catchError(this.formatErrors));
  }

  /**
   * 
   * @param path 
   * @param params 
   */
  delete<T>(path: string, params: Object = {}): Observable<T> {
    return this.http.delete<T>(this.urlApi + path, params).pipe(catchError(this.formatErrors));
  }

  ngOnDestroy(): void {
    if (this.subscription){
      this.subscription.unsubscribe();
    }
  }



}