import { Injectable } from '@angular/core';
import {Observable} from 'rxjs/Observable';
import {catchError, tap} from 'rxjs/operators';
import {AnswerThird} from './third-query/answer.third';
import {HttpClient} from '@angular/common/http';
import {of} from 'rxjs/observable/of';

@Injectable()
export class ThirdQueryService {
  private queryURL = 'http://localhost:9000/api/queries/third';
  constructor(private http: HttpClient) { }

  getAllAnswerThirds(address: string, saleDate: number): Observable<AnswerThird[]> {
    const url = `${this.queryURL}?address=${address}&saleDate=${saleDate}`;
    return this.http.get<AnswerThird[]>(url).pipe(
      tap(_ => console.log(`fetched all answers third`)),
      catchError(this.handleError<AnswerThird[]>(`getAllAnswerThirds`))
    );
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(`${operation} failed: ${error.message}`);

      return of(result as T);
    };
  }

}