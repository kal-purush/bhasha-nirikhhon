import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

import { Accounts } from '../models/Accounts';
import { TransferFunds } from '../models/TransferFunds';
import { environment } from  '../../environments/environment';
import { Observable, BehaviorSubject, of, throwError } from 'rxjs';
import { catchError, map, tap } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
@Injectable()
export class TransferFundsService {
  endpoint = environment.baseUrl;
  transferFunds:TransferFunds

    constructor(private http: HttpClient) { }

    
  getAccountDetails():Observable<any> {
   
     return  this.http.get<Accounts[]>(this.endpoint + "/accDetails/")
      .pipe(
          map(res => {
              return res;
          }),
          catchError(error => {
              return throwError(error);
          })
      );


  }
  

  getTransactionDetails(transferFunds):Observable<any> {
      return this.http.post(this.endpoint + "/transaction", transferFunds)
      .pipe(
          map(res => {
              return res;
        
          }),
          catchError(error => {
              return throwError(error);
          })
      );


  }
  
  
}