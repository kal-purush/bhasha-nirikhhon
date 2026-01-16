import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { of } from 'rxjs';
import { map, catchError } from 'rxjs/operators';
import { User } from '../models/user';

@Injectable()
export class UsersService {
  constructor(private http: HttpClient) {}

  getUsers() {
    return this.http.get('api/users').pipe(
      // map((res: any) => res.users),
      catchError(err => of(err))
    );
  }

  getUser(id: string) {
    return this.http.get('api/users/' + id).pipe(catchError(() => of({})));
  }

  createUser(user: User) {
    return this.http.post('api/users', user).pipe(catchError(err => of(err)));
  }
}