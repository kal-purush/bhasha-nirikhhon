import { Observable } from 'rxjs';
import { LoginService } from './login/login.service';
import { Http, RequestOptions, RequestOptionsArgs, Response } from '@angular/http';
import { Injectable } from '@angular/core';
import { AuthHttp, AuthConfig } from "angular2-jwt/angular2-jwt";

function handleUnauthorizedError(error: Response): Observable<any> {
  console.log(error);

  if (error.status === 401) {
    this.loginService.logout();
  }

  return Observable.empty();
}

@Injectable()
export class MDHTTP extends AuthHttp {
  constructor(options: AuthConfig, http: Http, defOpts: RequestOptions,
    private loginService: LoginService) {
    super(options, http, defOpts);
  }

    get(url: string, options?: RequestOptionsArgs): Observable<Response> {
    return super.get(url, options)
      .catch(handleUnauthorizedError.bind(this));
  }

  post(url: string, body: any, options?: RequestOptionsArgs): Observable<Response> {
    return super.post(url, body, options)
      .catch(handleUnauthorizedError.bind(this));
  }

  put(url: string, body: any, options?: RequestOptionsArgs): Observable<Response> {
    return super.put(url, body, options)
      .catch(handleUnauthorizedError.bind(this));
  }

  delete(url: string, options?: RequestOptionsArgs): Observable<Response> {
    return super.delete(url, options)
      .catch(handleUnauthorizedError.bind(this));
  }

  patch(url: string, body: any, options?: RequestOptionsArgs): Observable<Response> {
    return super.patch(url, body, options)
      .catch(handleUnauthorizedError.bind(this));
  }

  head(url: string, options?: RequestOptionsArgs): Observable<Response> {
    return super.head(url, options)
      .catch(handleUnauthorizedError.bind(this));
  }

  options(url: string, options?: RequestOptionsArgs): Observable<Response> {
    return super.options(url, options)
      .catch(handleUnauthorizedError.bind(this));
  }
}