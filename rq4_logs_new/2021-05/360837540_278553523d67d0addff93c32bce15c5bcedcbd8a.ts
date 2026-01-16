import { HttpEvent, HttpHandler, HttpInterceptor, HttpRequest } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { AuthService } from '../../user/shared/services';

@Injectable()
export class AuthInterceptor implements HttpInterceptor {
  constructor(private authService: AuthService) { }

  intercept(
    req: HttpRequest<any>,
    next: HttpHandler,
  ): Observable<HttpEvent<any>> {
    let headers = req.headers.set('Content-Type',  'application/json');

    if (this.authService.tokenFromLocalStorage) {
      headers = headers.set('Authorization', 'Bearer ' + this.authService.tokenFromLocalStorage);
    }

    const cloned = req.clone({ headers });
    return next.handle(cloned);
  }
}