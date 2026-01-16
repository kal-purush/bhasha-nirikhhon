import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders, HttpErrorResponse } from "@angular/common/http";
import { Observable, of, throwError } from 'rxjs';
import { awsUrl, localUrl } from 'src/environments/environment';
import { catchError, map } from "rxjs/operators";
import { Review } from '../models/review';

const url = localUrl + '/reviews';

@Injectable({
  providedIn: 'root'
})
export class ReviewService {

  constructor(private http: HttpClient) { }
  httpOptions = {
    headers: new HttpHeaders({ 'Content-Type': 'application/json' })
  }

  public addReview(review: Review): Observable<Review> {
    return this.http.post<Review>(`${url}`, review, this.httpOptions).pipe(catchError(this.handleError))
  }

  public findAllReviews(): Observable<Review[]> {
    return this.http.get<Review[]>(`${url}`)
      .pipe(
        catchError(this.handleError)
      )
  }

  public updateReview(review: Review, id: number): Observable<Review> {
    return this.http.put<Review>(`${url}/${id}`, review, this.httpOptions).pipe(catchError(this.handleError))
  }

  private handleError(httpError: HttpErrorResponse) {
    if (httpError instanceof ErrorEvent) {
      console.log('And error occurred: ', httpError);
    }
    else {
      console.error(`
        Backend returned code ${httpError.status},
        body was: ${httpError.error}
      `)
    }
    return throwError('Something bad happened; please try again later');
  }
}