import { Injectable } from '@angular/core';
import { IProduct } from '../product';
import {HttpClient, HttpErrorResponse} from '@angular/common/http'
import { Observable, throwError } from 'rxjs';
import {catchError, tap} from 'rxjs/operators'

@Injectable({
  providedIn: 'root'
})
export class ProductDataService {

  getProductsData() : Observable<IProduct[]>
  {
    return this.httpClientService.get<IProduct[]>("http://localhost:3000/products").pipe(tap(error => console.log("Error occured: " + JSON.stringify(error))),catchError(this.handleError));    
  }

  private handleError(error : HttpErrorResponse)
  {
    console.error(error);
    return throwError(error);

  }

  constructor(private httpClientService: HttpClient) {     
  }
}