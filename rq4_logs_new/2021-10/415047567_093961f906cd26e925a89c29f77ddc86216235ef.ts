import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders, HttpErrorResponse } from "@angular/common/http";
import { Observable, throwError } from 'rxjs';
import { User } from 'src/app/models/user';
import { awsUrl } from 'src/environments/environment';
import { catchError } from "rxjs/operators";

const url = awsUrl;

@Injectable({
  providedIn: 'root'
})
export class UserService {

  constructor(private http: HttpClient) { }
  httpOptions = {
    headers: new HttpHeaders({ 'Content-Type': 'application/json' })
  }
  public loginUser(user: User): Observable<User> {
    return this.http.post<User>(`${url}/user/login`, user, this.httpOptions);
  }

  public registerUser(user: User): Observable<User> {
    return this.http.post<User>(`${url}/user/add`, user, this.httpOptions).pipe(catchError(this.handleError))
  }

  public findAllUsers(): Observable<User[]> {
    return this.http.get<User[]>(url)
      .pipe(
        catchError(this.handleError)
      )
  }
  public findByUserId(id: number): Observable<User> {

    return this.http.get<User>(`${url}/${id}`)
      .pipe(
        catchError(this.handleError)
      );
  }
  public findByUsername(username: string): Observable<User> {

    return this.http.get<User>(`${url}/find/${username}`)
      .pipe(
        catchError(this.handleError)
      );
  }
  private handleError(httpError: HttpErrorResponse) {
    if (httpError instanceof ErrorEvent) {
      console.log('And error occurred: ', httpError);
    }
    else {
      console.error(`
        Backend returned code ${httpError.status},
        body was: ${httpError.error}
      `)
    }
    return throwError('Something bad happened; please try again later');
  }
}