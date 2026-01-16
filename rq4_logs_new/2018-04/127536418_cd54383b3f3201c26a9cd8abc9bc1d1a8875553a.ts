import { Injectable } from '@angular/core';
import {Observable} from 'rxjs/Observable';
import {catchError, tap} from 'rxjs/operators';
import {HttpClient} from '@angular/common/http';
import {of} from 'rxjs/observable/of';
import {AnswerFourth} from './fourth-query/answer.fourth';

@Injectable()
export class FourthQueryService {

  private queryURL = 'http://localhost:9000/api/queries/fourth';
  constructor(private http: HttpClient) { }

  getAllAnswerFourths(coffeeDrink: string, saleDate: number): Observable<AnswerFourth[]> {
    const url = `${this.queryURL}?coffeeDrink=${coffeeDrink}&saleDate=${saleDate}`;
    return this.http.get<AnswerFourth[]>(url).pipe(
      tap(_ => console.log(`fetched all answers fourth`)),
      catchError(this.handleError<AnswerFourth[]>(`getAllAnswerFourths`))
    );
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(`${operation} failed: ${error.message}`);

      return of(result as T);
    };
  }
}