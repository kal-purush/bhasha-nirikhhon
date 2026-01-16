import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { catchError } from 'rxjs/operators';
import { Observable, of } from 'rxjs';
import { environment } from '../environments/environment';

@Injectable({
  providedIn: 'root'
})
export class AuthenticationService {

  private rootUrl = environment.plannerBackendRootUrl;

  authenticated = false;

  constructor(private http: HttpClient) { }

  authenticate(credentials, callback) {
    const headers = new HttpHeaders(credentials ? {
      authorization: 'Basic ' + btoa(credentials.username + ':' + credentials.password)
    } : {});

    this.http.get(this.rootUrl + '/user', { headers: headers })
      .pipe(
        catchError(this.handleError('authenticate', null))
      )
      .subscribe(response => {
        if (response && response['name']) {
          this.authenticated = true;
        } else {
          this.authenticated = false;
        }
        return callback && callback();
      });
  }

  private handleError<T>(operation = 'operation', result ?: T) {
    return (error: any): Observable<T> => {
      this.authenticated = false;

      console.error(error);
      alert(`${operation} failed - check the console for the error`);

      return of(result as T);
    };
  }
}