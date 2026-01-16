import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Expense} from './models/expense.models';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})

export class ExpenseService {
  private dbUrl = 'https://tripn-server.herokuapp.com/';

  constructor(private http: HttpClient) {}

  getMyExpenses(userId, tripId, sessionToken): Observable<any> {
    const httpOptions = {
      headers: new HttpHeaders({
        "Content-Type": "application/json",
        Authorization: sessionStorage.getItem("sessionToken")
      })}
    return this.http.get(
      this.dbUrl+"expenses/tripexpenses/"+tripId+"/"+userId, httpOptions
    )
  }

  getSingleExpense() {}

  createExpense(expense: Expense, sessionToken) : Observable<any> {
    const httpOptions = {
      headers: new HttpHeaders({
        "Content-Type": "application/json",
        Authorization: sessionStorage.getItem("sessionToken")
      })}
    return this.http.post<any>(
      this.dbUrl+"expenses/createexpense", {expense: expense}, httpOptions
    );
  }

  editExpense() {}

  deleteExpense() {}
}