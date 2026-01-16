import { HttpClient, HttpErrorResponse, HttpHeaders } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, throwError } from 'rxjs';
import { catchError, retry } from 'rxjs/operators';
import { environment } from 'src/environments/environment';
import { Cliente } from '../domain/cliente';

@Injectable({
  providedIn: 'root'
})
export class ClienteService {

  constructor(private http: HttpClient) {}

  httpOptions = {
    headers: new HttpHeaders({ 'Content-Type': 'application/json' }),
  };

  private url: string = environment.baseUrl;
  private path: string = 'v1/clientes'

  getAllClientes(): Observable<Cliente[]> {
    return this.http
      .get<Cliente[]>(`${this.url}/${this.path}`)
      .pipe(retry(2), catchError(this.handleError));
  }

  getClienteById(id: number): Observable<Cliente> {
    return this.http
      .get<Cliente>(`${this.url}/${this.path}/${id}`)
      .pipe(retry(2), catchError(this.handleError));
  }

  getClienteByNome(nome: String): Observable<Cliente[]> {
    return this.http
      .get<Cliente[]>(`${this.url}/${this.path}?nome=${nome}`)
      .pipe(retry(2), catchError(this.handleError));
  }

  saveCliente(cliente: Cliente): Observable<Cliente> {
    return this.http
      .post<Cliente>(
        `${this.url}/${this.path}`,
        JSON.stringify(cliente),
        this.httpOptions
      )
      .pipe(retry(2), catchError(this.handleError));
  }

  updateCliente(cliente: Cliente): Observable<Cliente> {
    return this.http
      .put<Cliente>(
        `${this.url}/${this.path}/${cliente.id}`,
        JSON.stringify(cliente),
        this.httpOptions
      )
      .pipe(retry(1), catchError(this.handleError));
  }

  deleteCliente(id: number) {
    return this.http
      .delete<Cliente>(`${this.url}/${this.path}/${id}`, this.httpOptions)
      .pipe(retry(1), catchError(this.handleError));
  }

  handleError(error: HttpErrorResponse) {
    let errorMessage = '';
    if (error.error instanceof ErrorEvent) {
      // Erro ocorreu no lado do client
      errorMessage = error.error.message;
    } else {
      // Erro ocorreu no lado do servidor
      errorMessage =
        `Código do erro: ${error.status}, ` + `menssagem: ${error.message}`;
    }
    console.log(errorMessage);
    return throwError(errorMessage);
  }
}