import { Injectable } from '@angular/core';
import { HttpInterceptor, HttpRequest, HttpHandler, HttpEvent } from '@angular/common/http';
import { Observable } from 'rxjs';
import { AuthService } from './auth.service';


@Injectable({
  providedIn: 'root'
})
export class AuthInterceptorService implements HttpInterceptor {

  skip: Array<string>;

  constructor(private auth: AuthService) {
    this.skip = [
      '/auth/jwt',
      '/auth/users',
    ];
  }

  skipRequest(request: HttpRequest<any>) {
    var skip = false;
    this.skip.forEach(url => {
      if (request.url.includes(url)) {
        return skip = true;
      }
    });
    return skip;
  }

  intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    const token = this.auth.getToken();
    let request = req;
    if (this.skipRequest(request)) {
      return next.handle(request);
    }
    if (token) {
      request = req.clone({
        setHeaders: {
          authorization: `Bearer ${token}`
        }
      });
    }
    return next.handle(request);
  }

}