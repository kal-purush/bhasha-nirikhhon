import { Injectable } from '@angular/core';
import { timer } from 'rxjs';
import { map } from 'rxjs/operators';
import { User } from '../store/models/auth.model';

@Injectable({
  providedIn: 'root'
})
export class AuthService {
  private delay = 2000;
  private users: User[] = [{ username: 'asd', password: 'asd' }];

  constructor() {}

  signup(user: User) {
    return timer(this.delay).pipe(
      map(() => {
        const userNotExists = this.users.every(
          ({ username }) => username !== user.username
        );
        if (userNotExists) {
          this.users = [...this.users, user];
          return 'user signed up';
        }
        throw 'user already exists';
      })
    );
  }

  signin(user: User) {
    return timer(this.delay).pipe(
      map(() => {
        const userExists = this.users.some(
          ({ username, password }) =>
            username === user.username && password === user.password
        );
        if (userExists) {
          return user;
        }
        throw 'user does not exist';
      })
    );
  }

  signout() {
    return timer(this.delay).pipe(map(() => 'user signed out'));
  }
}