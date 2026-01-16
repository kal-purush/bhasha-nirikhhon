import {
  HttpErrorResponse,
  HttpEvent,
  HttpHandler,
  HttpInterceptor,
  HttpRequest,
  HttpResponse
} from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';

// Taken from https://github.com/angular/angular/issues/18680#issuecomment-346743360
// Remove when https://github.com/angular/angular/pull/18466 gets merged.
// Also: https://github.com/angular/angular/issues/18680

@Injectable()
export class WA18396Interceptor implements HttpInterceptor {
  constructor() {}

  intercept(
    req: HttpRequest<any>,
    next: HttpHandler
  ): Observable<HttpEvent<any>> {
    if (req.responseType === 'json') {
      req = req.clone({ responseType: 'text' });

      return next.handle(req).map(response => {
        if (response instanceof HttpResponse) {
          response = response.clone<any>({ body: JSON.parse(response.body) });
        }

        return response;
      });
    }

    return next.handle(req);
  }
}