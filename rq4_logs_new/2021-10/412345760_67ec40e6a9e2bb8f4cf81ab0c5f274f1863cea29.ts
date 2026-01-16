import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class AuthenticationService {

  constructor() { }

  //(email, password): Observable<any>
  login(email:any, password:any): any{
    //const query = `autenticar/login`;
    //const url = this.API_URL + query;

    if (email == "email@gmail.com" && password == 123456789) {
      return "torneus-token";
    }else{
      return false;
    }
    //return this.http.post<any>(url, formData, {'headers':this.headers});
  }

  isUserLoged(): boolean {
    const token: any = localStorage.getItem('cpen-token');
    //const isExpired = this.jwtHelper.isTokenExpired(accessToken);
    //console.log(accessToken)
    if (token) {
      this.setDataLogin(token);
      return true;
    } else {
      return false;
    }
  }

  setDataLogin(token:any): any{
    localStorage.setItem('torneus-token', token);
  }
}