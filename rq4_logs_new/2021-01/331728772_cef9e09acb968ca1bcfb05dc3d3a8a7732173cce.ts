import { Injectable } from '@angular/core';
import { HttpClient} from '@angular/common/http';
import { catchError } from 'rxjs/operators';
import { throwError } from 'rxjs';
import { HttpErrorResponse} from '@angular/common/http'

@Injectable({
  providedIn: 'root'
})
export class RegistrationService {



  constructor(private http: HttpClient) { }

  ngOnInit(): void {
  }

  postToServer(url, payload){
    return this.http.post(url, payload)
    .pipe(catchError(this.handleError))
  }

  handleError(error: HttpErrorResponse) {
    if (error.error instanceof ErrorEvent) {
      // A client-side or network error occurred. Handle it accordingly.
      console.error('An error occurred:', error.error.message);
    } else {
      // The backend returned an unsuccessful response code.
      // The response body may contain clues as to what went wrong.
      console.error(
        `${error.status}`
      );
      if(`${error.status}` === "409"){
        alert("Username already taken");
      }
      else{
        alert("insufficient funds");
      }
    }
    // Return an observable with a user-facing error message.
    return throwError(
      'Something bad happened; please try again later.');
  }
}