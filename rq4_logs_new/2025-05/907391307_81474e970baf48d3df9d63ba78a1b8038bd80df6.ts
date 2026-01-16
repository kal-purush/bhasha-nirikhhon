import { HttpClient, HttpHeaders, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from '../../../../environments/environment';

@Injectable({
  providedIn: 'root',
})
export abstract class BaseApiService {
  protected apiUrl = environment.apiUrl + '/api/v1';
  protected abstract resource: string;
  constructor(protected http: HttpClient) {}

  get<T>(endpoint: string | number, params?: HttpParams): Observable<T> {
    return this.http.get<T>(`${this.apiUrl}/${this.resource}/${endpoint}`, {
      params,
    });
  }

  post<T, R = T>(
    endpoint: string | number,
    body: T,
    headers?: HttpHeaders
  ): Observable<R> {
    return this.http.post<R>(
      `${this.apiUrl}/${this.resource}/${endpoint}`,
      body,
      { headers }
    );
  }

  put<T, R = T>(
    endpoint: string | number,
    body: T,
    headers?: HttpHeaders
  ): Observable<R> {
    return this.http.put<R>(
      `${this.apiUrl}/${this.resource}/${endpoint}`,
      body,
      { headers }
    );
  }

  delete<T>(endpoint: string | number, params?: HttpParams): Observable<T> {
    return this.http.delete<T>(`${this.apiUrl}/${this.resource}/${endpoint}`, {
      params,
    });
  }
}