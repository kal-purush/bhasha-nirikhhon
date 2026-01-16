import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

@Injectable({
  providedIn: 'root'
})
export class UsersService {

  private url = "http://85.160.64.233:3000/";


  constructor(
    private httpClient: HttpClient
  ) { }

  getUser() {
    return this.httpClient.get(this.url + "session/user");
  }

  getLogin() {
    return this.httpClient.get(this.url + "session/login");
  }

  getRegster() {
    return this.httpClient.get(this.url + "session/register");
  }

  getLogout() {
    return this.httpClient.get(this.url + "session/logout");
  }


}