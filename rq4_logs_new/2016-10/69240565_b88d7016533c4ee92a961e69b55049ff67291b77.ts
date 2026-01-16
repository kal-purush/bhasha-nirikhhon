import { Injectable } from '@angular/core';
import { CanActivate, Router, ActivatedRouteSnapshot, RouterStateSnapshot } from '@angular/router';

import { HttpService } from './http.service';

@Injectable()
export class AuthGuard implements CanActivate {
  isLoggedIn: boolean = false;
  redirectUrl: string;

  constructor(private httpService: HttpService, private router: Router) { }

  canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Promise<boolean> | boolean {
    let url: string = state.url;

    return this.authCheck(url);
  }

  authCheck(url: string): Promise<boolean> {
    this.redirectUrl = url;
    return this.httpService.get<any>('/user/authCheck').toPromise()
      .then((res) => {
        if(res.authed) {
          return true;
        } else {
          this.router.navigate(['login']);
        }
        return false;
      }).catch((error) => {
        this.router.navigate(['login']);
        return false;
      }
    )
  }
}