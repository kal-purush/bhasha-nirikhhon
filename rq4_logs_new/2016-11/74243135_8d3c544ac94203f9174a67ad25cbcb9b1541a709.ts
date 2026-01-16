//import { Injectable } from '@angular/core';
//import { Http, XHRBackend } from '@angular/http';
//import { Headers, RequestOptions, RequestOptionsArgs, Response } from '@angular/http';
//
//import { Observable } from 'rxjs/Observable';
//
//@Injectable()
//export class SecuredHttpService extends Http
//{
//    authorizationToken: string = "";
//
//    constructor(backend: XHRBackend, options: RequestOptions)
//    {
//        super(backend, options);
//    }
//
//    request(url: string, options?: RequestOptionsArgs): Observable<Response>
//    {
//        if (options)
//        {
//            options.headers.append('Authorization', this.authorizationToken);
//        } else
//        {
//            let headers = new Headers({ 'Authorization': this.authorizationToken });
//            let options = new RequestOptions({ headers: headers });
//        }
//
//        return super.request(url, options);
//    };
//
//}