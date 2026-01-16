import { Injectable } from '@angular/core';
import {
  HttpRequest,
  HttpHandler,
  HttpEvent,
  HttpInterceptor,
  HttpErrorResponse
} from '@angular/common/http';
import { Observable } from 'rxjs/Observable';

import { UserService } from './user.service';
import { catchError } from 'rxjs/operators';

@Injectable()
export class ErrorInterceptor implements HttpInterceptor {

  /**
   * Constructor of the interceptor.
   * @param userService UserService to provide token.
   */
  constructor(private userService: UserService) { }

  /**
   * If the token is set, adds authorization header to the request.
   * @param request httpRequest
   * @param next The next handler.
   */
  intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    return next.handle(request).pipe(
      catchError((err: HttpErrorResponse) => {
        if (err instanceof HttpErrorResponse) {
          if (navigator && !navigator.onLine) {
            alert('Nincs internet hozzáférése');
          } else {
            this.userService.signOut();
          }
        }
        return Observable.throw(err);
      })
    );
  }
}