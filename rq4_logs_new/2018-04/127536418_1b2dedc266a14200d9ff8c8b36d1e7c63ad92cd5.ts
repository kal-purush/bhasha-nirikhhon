import { Injectable } from '@angular/core';
import {Position} from './position/position';
import {HttpClient} from '@angular/common/http';
import {of} from 'rxjs/observable/of';
import {Observable} from 'rxjs/Observable';
import {catchError, tap} from 'rxjs/operators';

@Injectable()
export class PositionService {
  constructor(private http: HttpClient) { }

  private positionURL = 'http://localhost:9000/api/positions';

  getAllPositions(): Observable<Position[]> {
    const url = `${this.positionURL}/all`;
    return this.http.get<Position[]>(url).pipe(
      tap(_ => console.log(`fetched all positions`)),
      catchError(this.handleError<Position[]>(`getPositions`))
    );
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(`${operation} failed: ${error.message}`);

      return of(result as T);
    };
  }

}