import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs/Rx';

//取得データ型
import { User } from './User'

/*
  Generated class for the GitHubUsersServiceProvider provider.

  See https://angular.io/guide/dependency-injection for more info on providers
  and Angular DI.
*/
@Injectable()
export class GitHubUsersServiceProvider {

  constructor(public http: HttpClient) {
    console.log('Hello GitHubUsersServiceProvider Provider');
  }
  
  getUsers(): Observable<User[]>{
    console.log('GitHubUsersServiceProvider getUsers');
    return this.http.get<User[]>('https://api.github.com/users');
  }
  
}