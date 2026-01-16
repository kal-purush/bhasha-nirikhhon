import { Injectable } from '@angular/core';
import {catchError, tap} from 'rxjs/operators';
import {of} from 'rxjs/observable/of';
import {HttpClient} from '@angular/common/http';
import {Observable} from 'rxjs/Observable';
import {AnswerNinth} from './ninth-query/answer.ninth';

@Injectable()
export class NinthQueryService {

  private queryURL = 'http://localhost:9000/api/queries/ninth';
  constructor(private http: HttpClient) { }

  getAllAnswerNinths(fromDate: number): Observable<AnswerNinth[]> {
    const url = `${this.queryURL}?fromDate=${fromDate}`;
    return this.http.get<AnswerNinth[]>(url).pipe(
      tap(_ => console.log(`fetched all answers ninth`)),
      catchError(this.handleError<AnswerNinth[]>(`getAllAnswerNinths`))
    );
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(`${operation} failed: ${error.message}`);

      return of(result as T);
    };
  }

}