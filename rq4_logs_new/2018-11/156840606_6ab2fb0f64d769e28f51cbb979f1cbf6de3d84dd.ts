import { Injectable } from '@angular/core';
import {HttpEvent, HttpHandler, HttpInterceptor, HttpRequest} from '@angular/common/http';
import {Observable, throwError} from 'rxjs';
import {catchError} from 'rxjs/operators';
import {AuthenticationService} from '../services/authentication.service';


@Injectable({
  providedIn: 'root'
})
export class JwtInterceptorService implements HttpInterceptor {

    constructor(private _authService: AuthenticationService) {

    }

    intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
        const currentUser = this._authService.currentUserValue;
        if (currentUser && currentUser.token) {
            req = req.clone({
                setHeaders: {
                    Authorization: 'Bearer ' + currentUser.token
                }
            });
        }
        return next.handle(req)
            .pipe(
                catchError(err => {
                    if (err.status === 401) {
                        this._authService.logout();
                        location.reload(true);
                    }
                    const error = err.error.message || err.statusText;
                    return throwError(error);
                })
            );
    }

}