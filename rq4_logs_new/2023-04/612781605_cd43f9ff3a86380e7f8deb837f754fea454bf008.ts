import { Injectable } from '@angular/core';
import { JwtHelperService } from "@auth0/angular-jwt";

@Injectable({
  providedIn: 'root'
})
export class AuthService {

  token: string = "token";
  jwtHelper = new JwtHelperService();

  constructor() { }

  getToken() {
    //console.log('gettoken');
    if (this.isLoggedIn()) {
      return this.token;
    } else {
      this.logout();
    }
  }

  isLoggedIn(){
    return true;
    /* const token = localStorage.getItem('triage_user_token');
    if (!this.jwtHelper.isTokenExpired(token)) return true; */
  }

  logout(): void {
    /* const token = localStorage.getItem('triage_user_token');
    localStorage.removeItem('triage_user');
    localStorage.removeItem('triage_user_token');
    this.userObservableSource.next(false);
    this.http.post(this.domain + 'auth/logout?token=' + token, {}).subscribe(
      result=>{},
      error => {
        //console.log(error);             
      } 
    )
    this.router.navigate(['login']); */
  }
}