import 'rxjs/add/operator/toPromise';
import { Headers, Http } from '@angular/http';
import { Injectable } from '@angular/core';

@Injectable()
export class AuthService {
  private headers = new Headers({'Content-Type': 'application/X-www-form-urlencoded',
                                 'Access-Control-Allow-Credentials': 'true',
                                 'Access-Control-Allow-Origin': 'http://localhost:3000'});
  constructor(private http: Http) { }

  register(username: string, email: string, password: string) {
    const url = 'http://localhost:3000/auth/login';
    const data = 'username=' + username + '&email=' + email + '&password=' + password;

    return this.http.post(url, data, {headers: this.headers})
    .toPromise()
    .then((res) => {
        return new Promise((resolve, reject) => {
            console.log(res.headers);
            resolve(res);
        });
    });
  }

  login(username: string, password: string) {
    const data = 'username=' + username + '&password=' + password;
    const url = 'http://localhost:3000/auth/login';

    return this.http.post(url, data, {headers: this.headers})
    .toPromise()
    .then((res) => {
        return new Promise((resolve, reject) => {
          if (res.status === 200) {
            localStorage.setItem('currentUser', res.json().data[0]);
            console.log(localStorage.getItem('currentUser'));
            resolve(res);
            console.log("Logged in");
            console.log(res.headers);
          } else {
            reject(res);
          }
        });
    });
  }

  isLoggedIn() {
    const url = 'http://localhost:3000/auth/loggedIn';
  
    console.log('Calling isLoggedIn');
    return this.http.post(url, '', {headers: this.headers})
    .toPromise();
  }

}