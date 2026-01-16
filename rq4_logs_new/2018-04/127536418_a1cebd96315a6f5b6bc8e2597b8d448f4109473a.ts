import { Injectable } from '@angular/core';
import { CoffeeHouse } from './coffee-house/coffeeHouse';
import {Observable} from 'rxjs/Observable';
import {of} from 'rxjs/observable/of';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import {catchError, tap} from 'rxjs/operators';

const httpOptions = {
  headers: new HttpHeaders({ 'Content-Type': 'application/json' })
};

@Injectable()
export class CoffeeHouseService {

  constructor(private http: HttpClient) { }

  private coffeeHouseURL = 'http://localhost:9000/coffe-houses';


  getCoffeeHouse(address: string): Observable<CoffeeHouse> {
    const url = `${this.coffeeHouseURL}/Bratslavska%20104A`;
    return this.http.get<CoffeeHouse>(url).pipe(
      tap(_ => console.log(`fetched coffee house address=${address}`)),
      catchError(this.handleError<CoffeeHouse>(`getCoffeeHouse address=${address}`))
    );
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(`${operation} failed: ${error.message}`);

      return of(result as T);
    };
  }


}