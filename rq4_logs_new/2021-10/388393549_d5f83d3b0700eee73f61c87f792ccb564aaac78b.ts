import { AuthService } from './../auth/auth.service';
// import { apiConstants } from './api-constants';
import { Injectable } from '@angular/core';
import { } from '../../../environments/environment';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs/Observable';
import { catchError, map } from 'rxjs/operators';
import { ErrorObservable } from 'rxjs/observable/ErrorObservable';
import { EnvService } from '../env/env.service';

@Injectable({
  providedIn: 'root',
})
export class ApiInstrumentService {

  apiUrl: string;
  endpoint: string;
  endpointUrl: string;

  constructor(
    private authService: AuthService,
    private http: HttpClient,
    private envService: EnvService,
  ) {
    this.apiUrl = envService.apiUrl;
    this.endpointUrl = this.apiUrl + '/admin/';
  }

  // API: GET /course/:id
  public getInstrumentById(instrumentId: number): Observable<any> {
    return this.http
      .get(`${this.endpointUrl}instrument/${instrumentId}/`)
      .pipe(
        map((course: any) => {
          // console.log(course);
          if (course) return course;
          else throw course;
        }),
        catchError(this.handleError),
      );
  }

   // API: POST /instrument/
   public createInstrument(fields: any): Observable<any> {
    return this.http
      .post(`${this.endpointUrl}instrument/`, fields)
      .pipe(
        map((data: any) => {
          return data;
        }),
        catchError(this.handleError));
  }

  // API: PUT /instrument/:id/
  public updateInstrument(instrumentId: number, fields: any): Observable<any> {
    return this.http
      .put(`${this.endpointUrl}instrument/${instrumentId}/`, fields)
      .pipe(
        map((data: any) => {
          return data;
        }),
        catchError(this.handleError));
  }

  // API: GET /provider/
  public getProviders(instrumentId: number): Observable<any[]> {
    return this.http
      .get(`${this.endpointUrl}instrument/${instrumentId}/providers/`)
      .pipe(
        map((response: any) => {
          if (response?.results) return response.results;
          else return [];
        }),
        catchError(this.handleError),
      );
  }

   // API: POST /provider/
   public createProvider(instrumentId: number, fields: any): Observable<any> {
    return this.http
      .post(`${this.endpointUrl}instrument/${instrumentId}/provider/`, fields)
      .pipe(
        map((data: any) => {
          return data;
        }),
        catchError(this.handleError));
  }

  // API: PUT /provider/:id/
  public updateProvider(instrumentId: number, providerId: number, fields: any): Observable<any> {
    return this.http
    .post(`${this.endpointUrl}instrument/${instrumentId}/provider/${providerId}/`, fields)
      .pipe(
        map((data: any) => {
          return data;
        }),
        catchError(this.handleError));
  }

  private handleError(error: Response | any) {
    // console.error('ApiIcService::handleError', error);
    return ErrorObservable.create(error);
  }

}