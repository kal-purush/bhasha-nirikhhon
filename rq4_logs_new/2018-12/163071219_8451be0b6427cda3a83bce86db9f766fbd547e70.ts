import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { HttpParams, HttpClient } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { UserType } from '../models/enums/user-type.enum';

@Injectable({
  providedIn: 'root'
})
export class UserService {

  constructor(private _http: HttpClient) { }

  login(email: string, password: string): Observable<any> {
    return this._http.request('post', environment.apiUrl + '/login', {body : {
      email: email,
      password: password
    }});
  }

  register(email: string, password: string, username: string): Observable<any> {
    return this._http.request('post', environment.apiUrl + '/register', {body : {
      email: email,
      password: password,
      userType: UserType.VOLUNTEER,
      username: username
    }});
  }
}