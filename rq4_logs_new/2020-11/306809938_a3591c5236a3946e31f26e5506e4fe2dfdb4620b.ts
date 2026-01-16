import { Injectable } from '@angular/core';
import {
  HttpRequest,
  HttpHandler,
  HttpEvent,
  HttpInterceptor,
  HTTP_INTERCEPTORS
} from '@angular/common/http';
import { Observable } from 'rxjs';
import {JwtService} from '../service/jwt.service';

const HEADER_KEY = 'Authorization';

@Injectable()
export class AuthHttpInterceptor implements HttpInterceptor {

  constructor(private jwtService: JwtService) { }

  intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {

    let authRequest = request;
    const token = this.jwtService.getToken();

    if (token !== null) {
      authRequest = request.clone({
        headers: request.headers.set(
          HEADER_KEY,
          `Bearer ${token}`
        )
      });
    }
    return next.handle(authRequest);
  }
}

export const httpInterceptorProviders = [
  { provide: HTTP_INTERCEPTORS, useClass: AuthHttpInterceptor, multi: true }
];
