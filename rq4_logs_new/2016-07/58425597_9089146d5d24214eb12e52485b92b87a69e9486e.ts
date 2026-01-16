import { Injectable } from '@angular/core';
import { CanActivate,
         Router,
         ActivatedRouteSnapshot,
         RouterStateSnapshot } from '@angular/router';
import { SessionController } from 'angular2-devise-token-auth';

@Injectable()
export class AuthGuard implements CanActivate {
  constructor(private router: Router) {}

  canActivate(
    // Not using but worth knowing about
    next:  ActivatedRouteSnapshot,
    state: RouterStateSnapshot
  ) {
    console.log(SessionController.userSignedIn());
    if (SessionController.userSignedIn()) {
      return true;
    }

    this.router.navigate(['/sign-in']);
    return false;
  }
}