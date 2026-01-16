import { Injectable } from '@angular/core';
import {HttpEvent, HttpHandler, HttpInterceptor, HttpRequest, HttpResponse} from '@angular/common/http';
import {Observable} from 'rxjs';
import {AuthenticationService} from '../authentication/authenticationservice.service';
import {map} from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class HttpInterceptService implements HttpInterceptor {

  constructor(
    public authenticationService: AuthenticationService
  ) { }

  intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    const authHeader = this.authenticationService.getAuthenticatedToken();
    const username = this.authenticationService.getAuthenticatedUser();
    if (authHeader && username) {

      req = req.clone({
        setHeaders: {
          'Access-Control-Allow-Origin': '*',
          'Content-Type': 'application/json',
          // @ts-ignore
          Authorization: authHeader
        }
      });
    }
    // @ts-ignore
    return next.handle(req).pipe(
      map((event: HttpEvent<any>) => {
        if (event instanceof HttpResponse) {
          console.log(`event--->>> ${event}`);
        }
        return event;
      })
    );
  }
}