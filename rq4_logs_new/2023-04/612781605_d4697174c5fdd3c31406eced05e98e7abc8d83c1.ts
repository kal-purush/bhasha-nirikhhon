import { Injectable } from "@angular/core";
import {
  HttpEvent,
  HttpRequest,
  HttpHandler,
  HttpInterceptor,
  HttpErrorResponse
} from "@angular/common/http";
import { Observable, throwError } from "rxjs";
import { catchError, retry } from "rxjs/operators";
import { ToasterService } from "../shared/toaster/toaster.service";
import { paths } from "../data/paths";

@Injectable()
export class ErrorInterceptor implements HttpInterceptor {
  constructor(private toastr: ToasterService) {}
  intercept(
    req: HttpRequest<any>,
    next: HttpHandler
  ): Observable<HttpEvent<any>> {
    return next.handle(req).pipe(
      /* retry(2), */
      catchError((error: HttpErrorResponse) => {
        if (error.status !== 401) {
          // 401 handled in auth.interceptor
          this.toastr.show('error', 'Sorry!', 'Something went wrong. Please try later. ')
        }
        return throwError(error);
      })
    );
  }
}