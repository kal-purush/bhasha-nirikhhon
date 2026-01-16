/**
 * @author Hadi Horani, s144885
 */

import { Injectable } from '@angular/core';
import { HttpInterceptor, HttpHandler, HttpRequest, HttpEvent, HttpErrorResponse, HttpResponse } from '@angular/common/http';
import { Observable, throwError } from 'rxjs';
import { Router } from '@angular/router';
import { tap } from 'rxjs/operators';



@Injectable()
export class TokenInterceptor implements HttpInterceptor {

   constructor(public router: Router) {}

   intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {

    return next.handle(request).pipe(tap(
        (err: any) => {
          if (err instanceof HttpErrorResponse) {
              console.log('HTTP ERROR');
              if (err.status === 401) {
                this.router.navigate(['/login']);
               }
          }
        }
      ));
  }
}