import {HTTP_INTERCEPTORS, HttpEvent, HttpHandler, HttpInterceptor, HttpRequest} from "@angular/common/http";
import {Observable} from "rxjs";
import {TokenStorageService} from "../Components/component-vu/service/token-storage.service";
import set = Reflect.set;

export class AuthInterceptor implements HttpInterceptor {
  constructor(private tokenStorage: TokenStorageService) {
  }

  intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    let token = this.tokenStorage.getToken();
    if (token != null){
      console.log(this.tokenStorage.getToken())
      req.clone({headers: req.headers.set('Authorization', 'Bearer '  + this.tokenStorage.getToken())})

    }
    return  next.handle(req);
  }

}
export const httpInteceptorProvider = [{
   provide: HTTP_INTERCEPTORS, useClass: AuthInterceptor, multi: true
}]