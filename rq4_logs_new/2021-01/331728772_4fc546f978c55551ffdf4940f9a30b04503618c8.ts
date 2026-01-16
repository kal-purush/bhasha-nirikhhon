import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders, HttpErrorResponse} from '@angular/common/http';
import { BehaviorSubject} from 'rxjs';
import { Router } from '@angular/router';

const httpOption = {
  headers: new HttpHeaders({
    'Access-Control-Allow-Origin': '*',
    Authorization: ''
  })
};

@Injectable({
  providedIn: 'root'
})
export class LoginService {
  url: any = 'http:/locathost:8080';
  errorSubject: any = new BehaviorSubject<any>(null);
  errorMessage: any = this.errorSubject.asObservable();

  constructor(
    private http: HttpClient,
    private router: Router,
  ) {
  }

  login(Username: string, Password: string): any {
    this.http.post(`${this.url}/authenticate`, {username: Username, password: Password}, httpOption).toPromise().then((res: any) => {
      if (res.jwt && res.jwt) {
        sessionStorage.setItem('jwt', res.jwt);
        this.errorSubject.next(null);
        this.router.navigateByUrl('/dashboard');
      } else if (res.Message) {
        this.errorSubject.next(res.Message);
      }
    });
  }
}