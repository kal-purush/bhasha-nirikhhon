import { Injectable } from '@angular/core';
import { HttpEvent, HttpHandler, HttpInterceptor, HttpRequest } from '@angular/common/http';
import { Observable, throwError } from 'rxjs';
import { tap } from 'rxjs/operators';
import { AuthService } from '../common/auth.service';
import { ApiRequestConfigurationService } from './api-request-configuration.service';
import { NotificatorService } from '../common/notificator.service';
import { RPCError } from '@perun-web-apps/perun/models';

@Injectable()
export class ApiInterceptor implements HttpInterceptor {

  constructor(
    private authService: AuthService,
    private apiRequestConfiguration: ApiRequestConfigurationService,
    private notificator: NotificatorService,
  ) { }

  intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    // Apply the headers
    req = req.clone({
      setHeaders: {
        'Authorization': this.authService.getAuthorizationHeaderValue()
      }
    });

    // Also handle errors globally
    return next.handle(req).pipe(
      tap(x => x, err => {
        // Handle this err
        const errRpc = this.formatErrors(err, req);
        if (errRpc === undefined) {
          return throwError(err);
        }
        if (this.apiRequestConfiguration.shouldHandleError()) {
          this.notificator.showRPCError(errRpc);
        } else {
          return throwError(errRpc);
        }
      })
    );
  }

  private formatErrors(error: any, req: HttpRequest<any>) {
    let rpcError;
    if (error.error.errorId) {
      rpcError = error.error;
    } else {
      rpcError = JSON.parse(error.error) as RPCError;
    }
    if (rpcError === undefined) {
      return undefined;
    }
    rpcError.call = req.url;
    rpcError.payload = req.body;
    return rpcError;
  }
}