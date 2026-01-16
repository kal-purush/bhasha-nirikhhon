import { Injectable } from '@angular/core';
import {
  HttpRequest,
  HttpHandler,
  HttpEvent,
  HttpInterceptor,
  HttpResponse,
  HttpErrorResponse
} from '@angular/common/http';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/do';

@Injectable()
export class AuthHttpInterceptor implements HttpInterceptor {
  requestCounter = 0;
  constructor() {
  }

  intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {

    request = request.clone({
      responseType: 'json',
      setHeaders: {
        Authorization: `Token ${localStorage.getItem('token')}`
        // 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
      }
    });

    return next.handle(request).do((event: HttpEvent<any>) => {
      if (event instanceof HttpResponse) {
        // do stuff with response if you want
      }
    }, (err: any) => {
      if (err instanceof HttpErrorResponse) {
        // do stuff with response error if you want
      }
    });
  }
}