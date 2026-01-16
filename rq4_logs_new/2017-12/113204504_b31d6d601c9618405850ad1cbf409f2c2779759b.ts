import { Injectable } from '@angular/core';
import {
  HttpRequest,
  HttpHandler,
  HttpEvent,
  HttpInterceptor,
} from '@angular/common/http';
import { Observable } from 'rxjs/Observable';

@Injectable()
export class JWTInterceptors implements HttpInterceptor {

  intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    if (localStorage.getItem('token')) {
      const authReq = req.clone(
        {
          headers: req.headers.set('Authorization', localStorage.getItem('token'))
        });
      return next.handle(authReq);
    }
    return next.handle(req);
  }
}