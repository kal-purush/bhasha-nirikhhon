import { Injectable } from '@angular/core';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {API_URL} from '../../app.apiurl';
import {FormGroup} from '@angular/forms';
import {Params} from '@angular/router';

const httpOption = {
  headers: new HttpHeaders({
    'Access-Control-Allow-Origin': '*',
    Authorization: ''
  })
};

@Injectable({
  providedIn: 'root'
})
export class TransactionsService {

  constructor(private http: HttpClient) { }

  withdraw(transaction: FormGroup, url: Params): void{
    this.http.put<any>(`${API_URL}/myaccount/${url}/withdraw`, transaction, httpOption);
  }

  deposit(transaction: FormGroup, url: Params): void{
    this.http.put<any>(`${API_URL}/myaccount/${url}/deposit`, transaction, httpOption);
  }

  transfer(transaction: FormGroup, url: Params): void{
    this.http.put<any>(`${API_URL}/myaccount/${url}/transfer`, transaction, httpOption);
  }
}