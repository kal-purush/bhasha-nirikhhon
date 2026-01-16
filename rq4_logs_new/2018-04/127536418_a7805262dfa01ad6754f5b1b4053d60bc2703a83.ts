import { Injectable } from '@angular/core';
import {catchError, tap} from 'rxjs/operators';
import {AnswerSeventh} from './seventh-query/answer.seventh';
import {of} from 'rxjs/observable/of';
import {HttpClient} from '@angular/common/http';
import {Observable} from 'rxjs/Observable';

@Injectable()
export class SeventhQueryService {

  private queryURL = 'http://localhost:9000/api/queries/seventh';
  constructor(private http: HttpClient) { }

  getAllAnswerSevenths(address: string, avgSalary: number): Observable<AnswerSeventh[]> {
    const url = `${this.queryURL}?address=${address}&avgSalary=${avgSalary}`;
    return this.http.get<AnswerSeventh[]>(url).pipe(
      tap(_ => console.log(`fetched all answers seventh`)),
      catchError(this.handleError<AnswerSeventh[]>(`getAllAnswerSevenths`))
    );
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(`${operation} failed: ${error.message}`);

      return of(result as T);
    };
  }

}