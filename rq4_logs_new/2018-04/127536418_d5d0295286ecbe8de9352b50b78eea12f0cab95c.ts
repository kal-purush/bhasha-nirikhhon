import { Injectable } from '@angular/core';
import {Observable} from 'rxjs/Observable';
import {catchError, tap} from 'rxjs/operators';
import {HttpClient} from '@angular/common/http';
import {AnswerSecond} from './second-query/answer.second';
import {of} from 'rxjs/observable/of';

@Injectable()
export class SecondQueryService {


  private queryURL = 'http://localhost:9000/api/queries/second';
  constructor(private http: HttpClient) { }

  getAllAnswerSeconds(address: string, position: string, startDate: number): Observable<AnswerSecond[]> {
    const url = `${this.queryURL}?address=${address}&position=${position}&startDate=${startDate}`;
    return this.http.get<AnswerSecond[]>(url).pipe(
      tap(_ => console.log(`fetched all answers second`)),
      catchError(this.handleError<AnswerSecond[]>(`getAllAnswerSeconds`))
    );
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(`${operation} failed: ${error.message}`);

      return of(result as T);
    };
  }
}