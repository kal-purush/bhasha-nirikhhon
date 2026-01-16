import { Injectable } from '@angular/core';
import { SampleModel} from "./sample.class";
import { HttpClient, HttpErrorResponse, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs/Observable';
import { ErrorObservable } from 'rxjs/observable/ErrorObservable';
import { catchError, retry } from 'rxjs/operators';

@Injectable()
export class SampleService {

  constructor(private http: HttpClient) { }


  /****************
   * API REQUESTS *
   ****************/
  // an API request call can be reused, so it tends to be public and shared by a Service instead
  public getSample(): Observable<any> {
    return this.http.get('api/sample')
        .pipe(
            retry(3), // retry a failed request up to 3 times
            catchError(this.handleError) // then handle the error
        );
  }

  // PUT/POST requests are a bit more complicated, since they require a JSON payload body
  // return an Observable of type SampleModel
  public postSample(newFirstName: string, sampleModel: SampleModel): Observable<SampleModel> {
    // define JSON payload body
    let body = {
      newFirstName: newFirstName,
      lastName: sampleModel.lastName // lastName is unchanged
    };
    // define stubbed HTTP options
    const httpOptions = {
      headers: new HttpHeaders({
        'Content-Type': 'application/json',
        'Authorization': 'my-auth-token'
      })
    };
    return this.http.post<SampleModel>('api/sample', body, httpOptions)
        .pipe(
            catchError(this.handleError)
        );
  }


  /********************
   * HELPER FUNCTIONS *
   ********************/
  private handleError(error: HttpErrorResponse) {
    if (error.error instanceof ErrorEvent) {
      // a client-side or network error occurred. Handle it accordingly.
      console.error('An error occurred:', error.error.message);
    } else {
      // the backend returned an unsuccessful response code.
      // the response body may contain clues as to what went wrong,
      console.error(
          `Backend returned code ${error.status}, ` +
          `body was: ${error.error}`);
    }
    // return an ErrorObservable with a user-facing error message
    return new ErrorObservable(
        'Something bad happened; please try again later.');
  };
}