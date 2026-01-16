import { Injectable } from '@angular/core';
import {catchError, tap} from 'rxjs/operators';
import {of} from 'rxjs/observable/of';
import {HttpClient} from '@angular/common/http';
import {AnswerEight} from './eight-query/answer.eight';
import {Observable} from 'rxjs/Observable';

@Injectable()
export class EightQueryService {

  private queryURL = 'http://localhost:9000/api/queries/eighth';
  constructor(private http: HttpClient) { }

  getAllAnswerEights(birthdayDateS: number, birthdayDateE: number, endDate: number): Observable<AnswerEight[]> {
    const url = `${this.queryURL}?birthdayDateS=${birthdayDateS}&birthdayDateE=${birthdayDateE}&endDate=${endDate}`;
    return this.http.get<AnswerEight[]>(url).pipe(
      tap(_ => console.log(`fetched all answers eight`)),
      catchError(this.handleError<AnswerEight[]>(`getAllAnswerEights`))
    );
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(`${operation} failed: ${error.message}`);

      return of(result as T);
    };
  }

}