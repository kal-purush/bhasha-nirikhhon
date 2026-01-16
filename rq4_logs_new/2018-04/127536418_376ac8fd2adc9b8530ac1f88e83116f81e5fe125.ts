import { Injectable } from '@angular/core';
import {catchError, tap} from 'rxjs/operators';
import {of} from 'rxjs/observable/of';
import {HttpClient} from '@angular/common/http';
import {AnswerSixth} from './sixth-query/answer.sixth';
import {Observable} from 'rxjs/Observable';

@Injectable()
export class SixthQueryService {

  private queryURL = 'http://localhost:9000/api/queries/sixth';
  constructor(private http: HttpClient) { }

  getAllAnswerSixths(employee: string, saleDate: number): Observable<AnswerSixth[]> {
    const url = `${this.queryURL}?employee=${employee}&saleDate=${saleDate}`;
    return this.http.get<AnswerSixth[]>(url).pipe(
      tap(_ => console.log(`fetched all answers sixth`)),
      catchError(this.handleError<AnswerSixth[]>(`getAllAnswerSixths`))
    );
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(`${operation} failed: ${error.message}`);

      return of(result as T);
    };
  }

}