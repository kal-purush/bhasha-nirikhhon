import { Injectable } from '@angular/core';

import { Observable, throwError } from 'rxjs';
import {
  HttpClient,
  HttpHeaders,
  HttpErrorResponse
} from '@angular/common/http';
import { catchError, map } from 'rxjs/operators';

const url = 'https://dariocast.altervista.org/zama/menu.php';

@Injectable({
  providedIn: 'root'
})
export class MenuService {

  constructor(private http: HttpClient) { }

  private handleError(error: HttpErrorResponse) {
    if (error.error instanceof ErrorEvent) {
      // A client-side or network error occurred. Handle it accordingly.
      console.error('An error occurred:', error.error.message);
    } else {
      // The backend returned an unsuccessful response code. The response body may contain clues as to what went wrong,
      console.error(
        `Backend returned code ${error.status}, ` + `body was: ${error.error}`
      );
    }
    // return an observable with a user-facing error message
    return throwError(error);
  }

  private extractData(res: Response) {
    const menu = res;
    return menu || {};
  }

  public getMenu(): Observable<any> {

    // Call the http GET
    return this.http.get(url).pipe(
      map(this.extractData),
      catchError(this.handleError)
    );
  }

}