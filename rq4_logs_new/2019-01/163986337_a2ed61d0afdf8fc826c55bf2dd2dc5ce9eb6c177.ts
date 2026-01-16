import { Injectable } from '@angular/core';
import {
  HttpRequest,
  HttpHandler,
  HttpEvent,
  HttpInterceptor
} from '@angular/common/http';
import { Observable } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { Store } from '@ngrx/store';
import * as _ from 'lodash';

import { NotificationType } from '../components/notification/notification.model';
import { AppState } from '../store/app.reducers';
import { SetNotification } from '../components/notification/store/notification.actions';

const DEFAULT_ERROR_MESSAGE = 'Oops, something went terribly wrong!';

@Injectable()
export class ErrorInterceptor implements HttpInterceptor {

  constructor(
    private store: Store<AppState>
  ) {}

  intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    
    return next.handle(request)
      .pipe(
        catchError((httpErrorResponse: any) => {
          if (httpErrorResponse.status === 0 || httpErrorResponse.status >= 500) {
            const message = _.get(httpErrorResponse, 'error.error') || DEFAULT_ERROR_MESSAGE;
            this.store.dispatch(new SetNotification({ message, type: NotificationType.error }));
          }
          throw httpErrorResponse;
        })
      );

  }

}