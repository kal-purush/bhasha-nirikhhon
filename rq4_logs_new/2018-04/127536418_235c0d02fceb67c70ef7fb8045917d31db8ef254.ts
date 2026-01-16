import { Injectable } from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {Product} from './product/product';
import {of} from 'rxjs/observable/of';
import {Observable} from 'rxjs/Observable';
import {catchError, tap} from 'rxjs/operators';

@Injectable()
export class ProductService {
  constructor(private http: HttpClient) { }

  private productURL = 'http://localhost:9000/api/products';

  getAllProducts(): Observable<Product[]> {
    const url = `${this.productURL}/all`;
    return this.http.get<Product[]>(url).pipe(
      tap(_ => console.log(`fetched all products`)),
      catchError(this.handleError<Product[]>(`getProducts`))
    );
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(`${operation} failed: ${error.message}`);

      return of(result as T);
    };
  }
}