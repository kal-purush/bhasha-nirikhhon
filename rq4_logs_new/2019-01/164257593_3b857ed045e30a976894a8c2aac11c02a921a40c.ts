import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { transaction } from './transactions';

const httpOptions = {
  headers: new HttpHeaders({ 'Content-Type': 'application/json' })
};

@Injectable({
  providedIn: 'root'
})
export class TransactionsService {
  private Url = 'http://localhost:8080/api/';  // URL to web api

  constructor(private http: HttpClient) { }
 
  getAllTransactions (): Observable<transaction[]> {
//return this.http.get<transaction[]>(this.Url+'allTransactions')
return this.http.get<transaction[]>(this.Url+'allTransactions')
  }

  addTransaction (asset: transaction): Observable<transaction> {
    return this.http.post<transaction>(this.Url+'Transaction', asset, httpOptions);
  }
}