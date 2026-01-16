import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http'; 
import { TokenParams } from '../classes/token-params';

@Injectable()
export class AuthService {

  AccessToken: String = "";
  constructor(private http: HttpClient) { }

  private TokenApi = 'http://localhost:3000/api/authenticate';
 
  login(email: string, password: string) {
    var data = {
        email : email, 
        password: password
    }
    return this.http.post<TokenParams>( this.TokenApi, data).subscribe(
        res => {
            console.log(res);
        }, 
        err => {
            console.log(err);
        }
    )

}

logout() {
    // remove user from local storage to log user out
    localStorage.removeItem('currentUser');
}
}