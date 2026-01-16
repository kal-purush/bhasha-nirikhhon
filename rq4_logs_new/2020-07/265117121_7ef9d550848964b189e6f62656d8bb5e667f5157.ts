import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse, HttpHeaders } from '@angular/common/http';

import { throwError, Observable, of } from 'rxjs';
import { catchError, tap, map } from 'rxjs/operators';

import { Invoice } from './invoice';

import { environment } from '../../../environments/environment';

const httpOptions = {
headers: new HttpHeaders({'Content-Type': 'application/json'})
};
const apiUrl = environment.production ? '/api/invoices/' : 'http://localhost:3000/api/invoices';

@Injectable({
  providedIn: 'root'
})
export class InvoiceService {

  constructor(private httpClient: HttpClient) { }

  addInvoice(invoice: Invoice): Observable<Invoice> {
    return this.httpClient.post<Invoice>(apiUrl, invoice, httpOptions);
  }

  getAll(): Observable<Invoice[]> {
    return this.httpClient.get<Invoice[]>(apiUrl)
    .pipe(
      catchError(this.handleError('getAll', []))
    );
  }

  getInvoiceById(id: string): Observable<Invoice> {
    const url = `${apiUrl}/${id}`;
    return this.httpClient.get<Invoice>(url).pipe(
      tap(_ => console.log(`fetched invoice id=${id}`)),
      catchError(this.handleError<Invoice>(`getInvoiceById id=${id}`))
    );
  }

  getAllByUserId(userId: string): Observable<Invoice[]> {
    const url = `${apiUrl}/user/${userId}`;
    return this.httpClient.get<Invoice[]>(url)
    .pipe(
      catchError(this.handleError('getAllByUserId', []))
    );
  }

  deleteInvoice(id: string): Observable<Invoice> {
    const url = `${apiUrl}/${id}`;
    return this.httpClient.delete<Invoice>(url, httpOptions).pipe(
      tap(_ => console.log(`deleted invoice id=${id}`)),
      catchError(this.handleError<Invoice>('deleteInvoice'))
    );
  }

  cancelInvoice(id: string): Observable<Invoice> {
    const url = `${apiUrl}/${id}/cancel`;
    return this.httpClient.put<Invoice>(url, httpOptions).pipe(
      tap(_ => console.log(`cancelled invoice id=${id}`)),
      catchError(this.handleError<Invoice>('cancelInvoice'))
    );
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {

    // TODO: send the error to remote logging infrastructure
    console.error(error); // log to console instead

    // Let the app keep running by returning an empty result.
    return of(result as T);
    };
  }
}