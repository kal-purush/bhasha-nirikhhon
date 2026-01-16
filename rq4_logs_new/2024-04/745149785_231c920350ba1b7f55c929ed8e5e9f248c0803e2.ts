import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, of } from 'rxjs';
import { catchError, map, switchMap, timeout } from 'rxjs/operators';
import { interval } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ConnectivityService {

  constructor(private http: HttpClient) { }

   checkInternetConnectivity(): Observable<boolean> {
     return this.http.get('https://httpstat.us/200', { observe: 'response' }).pipe(
      timeout(3000), // Timeout de 5 segundos
      map(response => response.status === 200),
      catchError(error => of(false))
    );
   }

  //  checkInternetConnectivity(): Observable<boolean> {
  //    return interval(10000).pipe( // Intervalo de 10 segundos
  //      switchMap(() =>
  //        this.http.get('https://httpstat.us/200', { observe: 'response' }).pipe(
  //          timeout(5000), // Timeout de 5 segundos
  //          map(response => response.status === 200),
  //          catchError(error => of(false))
  //        )
  //      )
  //    );
  //  }
}