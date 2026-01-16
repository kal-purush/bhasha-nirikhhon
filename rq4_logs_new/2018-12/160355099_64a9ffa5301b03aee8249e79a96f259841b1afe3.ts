import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { map } from 'rxjs/operators';


@Injectable({
providedIn: 'root'
})

export class JwtService {
    constructor(private httpClient: HttpClient) { }

  signin(email:string, password:string) {
    localStorage.setItem('email',email);
      return this.httpClient.post<{access_token:  string}>('https://localhost:44398/api/LogIn',{email, password} )
      .pipe(map(value => {
      localStorage.setItem('access_token',JSON.stringify(value.access_token));
  }))
  }
  
  signout() {
    localStorage.removeItem('access_token');
  }

  public get loggedIn(): boolean{
    return localStorage.getItem('access_token') !==  null;
  }
  
  
}