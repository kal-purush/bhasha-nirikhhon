import {Injectable} from '@angular/core';
import {map} from 'rxjs/operators';
import {environment} from '../../environments/environment';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {BehaviorSubject, Observable} from 'rxjs';
import {User} from '../_models/user';
import * as jwt_decode from 'jwt-decode';

@Injectable({
  providedIn: 'root'
})
export class AnonymService {
  private currentAnonymSubject: BehaviorSubject<User>;
  public currentAnonym: Observable<User>;

  url = environment.apiUrl;
  httpOptions = {
    headers: new HttpHeaders({
      'Content-Type': 'application/json',
      observe: 'response'
    })
  };

  constructor(private http: HttpClient) {
    this.currentAnonymSubject = new BehaviorSubject<User>(
      localStorage.getItem('anonymData') ? jwt_decode(localStorage.getItem('anonymData')) : undefined);
    this.currentAnonym = this.currentAnonymSubject.asObservable();
  }

  anonymLogin(username: string) {
    return this.http.post(`${this.url}anonym`, {},
      {headers: this.httpOptions.headers, params: {username}}).pipe(
      map(data => {
          const tokenJSON: any = data;
          localStorage.setItem('anonymData', tokenJSON.token);
          const userDecode: User = jwt_decode(tokenJSON.token);
          console.log(userDecode);
          this.currentAnonymSubject.next(userDecode);
          return userDecode;
        }
      )
    );
  }

  public get currentAnonymValue(): User {
    return this.currentAnonymSubject.value;
  }

  removeAnonym(): void {
    localStorage.removeItem('anonymData');
    localStorage.removeItem('sessionid');
    this.currentAnonymSubject.next(null);
  }
}