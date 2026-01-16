import { Injectable } from '@angular/core';
import {CanLoad, Route, Router, UrlSegment} from '@angular/router';
import { Observable } from 'rxjs';
import {AuthService} from './auth.service';

@Injectable({
  providedIn: 'root'
})
export class AuthGuard implements  CanLoad {
  constructor(private authservice: AuthService, private router: Router) {}
  canLoad(
      route: Route, segmnets: UrlSegment[]): Observable<boolean> | Promise<boolean> | boolean {
    if (!this.authservice.userAuthenticated) {
      this.router.navigateByUrl('/auth');
    }
return this.authservice.userAuthenticated;
  }
}