import { Injectable } from '@angular/core';
import {
  HttpInterceptor,
  HttpEvent,
  HttpHandler,
  HttpRequest
} from '@angular/common/http';
import { Observable } from 'rxjs';

import { environment } from '@env/environment';

@Injectable()
export class ApiUrlInterceptor implements HttpInterceptor {
  intercept(
    req: HttpRequest<any>,
    next: HttpHandler
  ): Observable<HttpEvent<any>> {
    // Skip interceptor if request is for assets folder
    if (req.url.includes('assets')) {
      return next.handle(req);
    }
    const baseUrl = environment.api.baseUrl;
    const apiReq = req.clone({ url: `${baseUrl}${req.url}` });

    return next.handle(apiReq);
  }
}