import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

export interface User {
  id?: number;
  name?: string;
  username?: string;
  email?: string;
  address?: string;
  phone?: string;
  website?: string;
  company?: string;
}
export interface Post {
  userId?: number;
  id?: number;
  title?: string;
  body?: string;
}

@Injectable({
  providedIn: 'root'
})
export class ApiService {
  private _http = inject(HttpClient);
  private _baseURL = 'https://jsonplaceholder.typicode.com';
  private _userUrl = `${this._baseURL}/users`;
  private _postUrl = `${this._baseURL}/posts`;


  getUsers$(): Observable<User[]> {
    return this._http.get<User[]>(this._userUrl);
  }

  getPosts$(): Observable<Post[]> {
    return this._http.get<Post[]>(this._postUrl);
  }
}