import { Injectable } from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {of} from 'rxjs/observable/of';
import {Observable} from 'rxjs/Observable';
import {catchError, tap} from 'rxjs/operators';
import {AnswerFirst} from './first-query/answer.first';

@Injectable()
export class FirstQueryService {

  private queryURL = 'http://localhost:9000/api/queries/first';
  constructor(private http: HttpClient) { }

  getAllAnswerFirsts(address: string, productName: string): Observable<AnswerFirst[]> {
    const url = `${this.queryURL}?productName=${productName}&address=${address}`;
    return this.http.get<AnswerFirst[]>(url).pipe(
      tap(_ => console.log(`fetched all answers first`)),
      catchError(this.handleError<AnswerFirst[]>(`getAllAnswerFirsts`))
    );
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(`${operation} failed: ${error.message}`);

      return of(result as T);
    };
  }

}