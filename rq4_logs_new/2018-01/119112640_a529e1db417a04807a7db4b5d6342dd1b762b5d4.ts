import { Injectable } from "@angular/core";
import { HttpInterceptor } from "@angular/common/http/src/interceptor";
import { HttpHandler, HttpEvent, HttpRequest, HttpHeaders } from '@angular/common/http';
import { Observable } from "rxjs/Observable";
import { LocalStorageService } from '../services/local-storage.service';


@Injectable()
export class AuthIntercepter implements HttpInterceptor{

    constructor(private localStorageService: LocalStorageService) {}

    intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
        if(req.url.indexOf('/api/authenticate')>0){
            var headersForTokenApi = new HttpHeaders({ 'Content-Type':'application/x-www-form-urlencoded' });
            // console.log(req);
            return next.handle(req);
        }

        var authHeader = this.localStorageService.GetAuthorizationData().token;
        var authReq = req.clone({ setHeaders: { Authorization: authHeader } });
        // console.log(req);

        return next.handle(authReq);
    }
}