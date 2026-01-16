import { Injectable } from '@angular/core';
import {
    HttpRequest,
    HttpHandler,
    HttpEvent,
    HttpInterceptor
} from '@angular/common/http';

import * as config from '../environments/config'

import { Select } from '@ngxs/store';

import { Observable } from 'rxjs/Observable';

import { HttpWrapper } from '../common/http-wrapper';
import { AuthStateModel, JwtInfoDto } from 'src/ngxs/models/authState.model';
import { tap } from 'rxjs/operators';

@Injectable()
export class TokenInterceptor implements HttpInterceptor {
    @Select(state => state.auth) authInfo$: Observable<AuthStateModel>;
    constructor(private httpWrapper: HttpWrapper<JwtInfoDto>) { }
    intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
        this.httpWrapper.isInRequest = true
        this.authInfo$.subscribe((response: AuthStateModel) => {
            const { token } = response;
            request = request.clone({
                setHeaders: {
                    Authorization: `${config.configFile.scheme} ${token}`
                }
            })
        })
        return next.handle(request)
            .pipe(
                tap(err => {
                    this.httpWrapper.isInRequest = false
                    return Observable.of(err)
                }),
                tap(response => {
                    if (response.type) {
                        this.httpWrapper.isInRequest = false
                    }
                    return response
                })
            )
    }
}