import { Injectable } from '@angular/core';
import {Observable} from 'rxjs/Observable';
import {catchError, tap} from 'rxjs/operators';
import {HttpClient} from '@angular/common/http';
import {of} from 'rxjs/observable/of';
import {AnswerFifth} from './fifth-query/answer.fifth';

@Injectable()
export class FifthQueryService {

  private queryURL = 'http://localhost:9000/api/queries/fifth';
  constructor(private http: HttpClient) { }

  getAllAnswerFifths(date: number, salary: number): Observable<AnswerFifth[]> {
    const url = `${this.queryURL}?date=${date}&salary=${salary}`;
    return this.http.get<AnswerFifth[]>(url).pipe(
      tap(_ => console.log(`fetched all answers fifth`)),
      catchError(this.handleError<AnswerFifth[]>(`getAllAnswerFifths`))
    );
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(`${operation} failed: ${error.message}`);

      return of(result as T);
    };
  }

}