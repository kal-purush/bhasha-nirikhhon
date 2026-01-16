import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {Observable} from 'rxjs';
import {map} from 'rxjs/operators';
import {Token} from './models/token.interface';

@Injectable({
  providedIn: 'root'
})
export class AuthenticationService {

  private url: "http://85.160.64.233:3000/";
  private token: string | null = null;

  public get isLoggedIn() {
    return this.token !== null;
  }

  constructor(
    private httpClient: HttpClient
  ) {
  }

  public getToken(): string {
    return this.token;
  }

  public getLogin(email: string, password: string): Observable<Token> {
    return this.httpClient.post<Token>('http://85.160.64.233:3000/session/login', {
      email, password
    }).pipe(
      map<Token, Token>(i => {
        this.token = i.access_token;
        return i;
      })
    );
  }

  public getRegister(username: string, email: string, password: string, passwordConfirm: string): Observable<void> {
    return this.httpClient.post<void>(this.url + '/session/register', {
      username,
      email,
      password,
      password_confirmation: passwordConfirm,
    });
  }

}