import { Injectable, Injector } from "@angular/core"
import {
  HttpEvent,
  HttpInterceptor,
  HttpHandler,
  HttpRequest
} from "@angular/common/http"
import { Observable } from "rxjs"

import { AuthService } from "./services/auth/auth.service"
import { Router } from "@angular/router"
@Injectable()
export class AuthInterceprot implements HttpInterceptor {
  constructor(private _auth: AuthService, private _router: Router) {}

  intercept(
    req: HttpRequest<any>,
    next: HttpHandler
  ): Observable<HttpEvent<any>> {
    if (this._auth.isLogin()) {
      // const idToken = this.presentA.getSession()
      const authReq = req.clone({
        headers: req.headers.set("Authorization", "sukum-lnopq")
      })
      return next.handle(authReq)
    } else {
      return next.handle(req)
    }
    // return next.handle("")
    // const token_ToServer =
    // console.log(idToken)
  }
}