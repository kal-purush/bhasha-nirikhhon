import { Injectable } from '@angular/core';

import { Observable, of} from 'rxjs';
import { HttpClient, HttpHeaders, HttpErrorResponse } from '@angular/common/http';
import { catchError, tap, map } from 'rxjs/operators';
import { FormGroup } from '@angular/forms';
import {AppUser} from './app-user'
import { stringify } from 'querystring';
import { JsonPipe } from '@angular/common';

@Injectable({  
  providedIn: 'root'  
}) 
export class UserService {

  constructor(private http : HttpClient ) { }
  private newMethod() {
    return 'root';
  }
  Url:string = 'https://apiforhouseholdrapitup20190308031732.azurewebsites.net/Api/';
  countryApi: string='https://restcountries.eu/rest/v2/all';
RegisterUser(user: AppUser): Observable<AppUser>{
if(user.Username!=null){
  return this.http.post<AppUser>(
    this.Url+'Account/Register',
    user,
    {
      headers: new HttpHeaders({'Content-Type':'application/json' })
    }
  ).pipe(
    tap((user:AppUser) => console.log(`added User w/ id=${user.Username}`)),
    catchError(this.handleError<AppUser>('RegisterUser'))
  );
}



}

public getCountry():Observable<object>{
return this.http.get<JSON>(this.countryApi).pipe(
  tap((data:object) => console.log(`added User w/ id=${data}`)),
  catchError(this.handleError<object>('RegisterUser'))
);;
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