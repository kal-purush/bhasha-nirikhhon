import { Injectable } from '@angular/core';
import {User} from '@app/shared/class/user';
import { environment } from '@environments/environment';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {catchError, map, tap} from 'rxjs/operators';
import {Observable, of} from 'rxjs';

const domainServer = environment.domain_server;
const httpOptions = {
  headers: new HttpHeaders({ 'Content-Type': 'application/json' })
};

@Injectable({
  providedIn: 'root'
})
export class UserService {

  constructor(public http: HttpClient) { }

  createUser(user: User): Observable<User> {
    return this.http.post<User>(`${domainServer}/api/users`, {
      userName: user.userName,
      profileType: user.profileType,
      dateOfBirth: user.dateOfBirth,
      email: user.email.trim().toLowerCase(),
      password: user.password,
    });
  }
}