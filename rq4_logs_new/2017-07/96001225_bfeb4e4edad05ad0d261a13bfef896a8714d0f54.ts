import { Injectable } from '@angular/core';
import { Http, Response } from '@angular/http';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/do';
import 'rxjs/add/operator/catch';
import { Observable } from 'rxjs/Observable';

import { User } from '../../models/user';


@Injectable()
export class GithubServiceProvider {
  
  githubApiUrl = 'https://api.github.com';

  constructor(public http: Http) {
    console.log('Hello GithubServiceProvider Provider');
  }
  
  load(): Observable<User[]>{
    return this.http.get(`${this.githubApiUrl}/users`)
    .map((res: Response) => <User[]>res.json())
  }

  loadDetails(login: string): Observable<User> {
    return this.http.get(`${this.githubApiUrl}/users/${login}`)
    .map(res => <User>(res.json()))
  }
  
}