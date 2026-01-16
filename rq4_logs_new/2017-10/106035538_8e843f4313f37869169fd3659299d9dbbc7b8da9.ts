import { Injectable } from '@angular/core';
import { Subject } from 'rxjs/Subject';
import { User } from '../models/user';
import { DataService } from '../data.service';
import { Observable } from 'rxjs/Observable';

import 'rxjs/add/operator/do';

@Injectable()
export class FunFastUserService {

  public userChanged: Subject<User>;
  public currentUser: User;

  constructor(private dataService: DataService) {
    this.userChanged = new Subject<User>();
  }

  login(username: string, password: string): Observable<User> {
    return null;
  }

  logout(): Observable<boolean> {
    return null;
  }

  signup(username: string, password: string): Observable<User> {
    return this.dataService
      .addRecord('users/new', { username, password })
      .do(user => this.userChanged.next(user))
      .do(user => this.currentUser = user);
  }

}