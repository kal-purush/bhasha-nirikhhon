import { Injectable } from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {Contract} from './contract/contract';
import {of} from 'rxjs/observable/of';
import {Observable} from 'rxjs/Observable';
import {catchError, tap} from 'rxjs/operators';

@Injectable()
export class ContractService {

  constructor(private http: HttpClient) { }

  private contractURL = 'http://localhost:9000/api/contracts';

  getAllContracts(): Observable<Contract[]> {
    const url = `${this.contractURL}/all`;
    return this.http.get<Contract[]>(url).pipe(
      tap(_ => console.log(`fetched all contracts`)),
      catchError(this.handleError<Contract[]>(`getContracts`))
    );
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(`${operation} failed: ${error.message}`);

      return of(result as T);
    };
  }
}