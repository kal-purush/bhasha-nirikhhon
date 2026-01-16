import { Injectable } from '@angular/core';
import { HttpRequest, HttpHandler, HttpEvent, HttpInterceptor } from '@angular/common/http';
import { Observable, throwError } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { environment } from '@environments/environment';
import { ToastController } from '@ionic/angular';

const domainServer = environment.domain_server;

@Injectable({
  providedIn: 'root',
})
export class ErrorInterceptor implements HttpInterceptor {
  constructor(private toastController: ToastController) {}

  intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    return next.handle(request).pipe(
      catchError(err => {
        // catch server down
        if (err.status === environment.status_code_server_down && request.url.startsWith(domainServer)) {
          const toast = this.toastController.create({
            message: 'Oups.. on rencontre un soucis venaient réessayer plus tard. 😥',
            duration: 5000,
          });
          toast.then(data => data.present());
        }

        return throwError(err);
      }),
    );
  }
}