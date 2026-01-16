import { Injectable } from '@angular/core';
import { Router, CanActivate } from '@angular/router';
import {HttpClient } from '@angular/common/http'
import { Observable } from 'rxjs/Observable';

@Injectable()
export class SiteRegisterGuard implements CanActivate {

    constructor(private router: Router,
    private http: HttpClient) { }

    canActivate(): Observable<boolean> {
        return this.http.get('api/site/canRegisterSite')
        .map((response: boolean) => {
            console.log(JSON.stringify(response));
            if(!response){
                this.router.navigate(['login']);
            }
            return response;
        })
    }
}