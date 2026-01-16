import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, CanActivate, RouterStateSnapshot } from '@angular/router';
@Injectable()
export class BackingUpFromResultsNotAllowedGuard implements CanActivate {

  lastPageResults = false;

  constructor() { }

  canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): boolean {
    console.log('Route: ' + route.url.toString());
    const access = !this.lastPageResults || route.url.toString().indexOf('kysymykset') === -1;
    if (access) {
      this.lastPageResults = state.url.toString().indexOf('tulokset') !== -1;
    }
    return access;
  }
}