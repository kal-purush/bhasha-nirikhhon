import { HttpEvent, HttpHandler, HttpInterceptor, HttpRequest, HttpErrorResponse } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { retry, catchError } from "rxjs/operators";
import { Router } from "@angular/router";


@Injectable({
  providedIn: 'root'
})
export class BasicAuthService implements HttpInterceptor{

  constructor(private router: Router) { }

  intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
       req = req.clone({
        setHeaders:{
          Authorization:`Basic YWRtaW46MTIzNA==`
        }
      });
      return next.handle(req).pipe(
        retry(1),
        catchError((e: HttpErrorResponse) => {
            
            if(e.status != 200){
            this.router.navigateByUrl('/not-found');
        }
        throw e;
        })
    );
  }

}