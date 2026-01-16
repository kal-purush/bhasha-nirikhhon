import { Injectable } from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {SettingsService} from '../../shared/services/settings.service';
import {ITransaction} from '../models/Transaction.model';

@Injectable({
  providedIn: 'root'
})
export class TransactionsService {

  constructor(private http: HttpClient,
              private settings: SettingsService) {}

  get transactionsApi(): string {
    return this.settings.apiHost + 'transactions/';
  }

  addTransaction(task) {
    return this.http.post<{ message: string; result: ITransaction }>(this.transactionsApi, task);
  }

  getTransactions() {
    return this.http.get<{ message: string; result: ITransaction[] }>(this.transactionsApi);
  }

  // TODO Handle updates and deletes
  updateTransactions(update) {
    return this.http.put<{ message: string; result: ITransaction }>(this.transactionsApi, update);
  }

  deleteTransactions() {
    return this.http.delete<{ message: string; result: ITransaction[] }>(this.transactionsApi);
  }
}