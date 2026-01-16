import { Injectable } from '@angular/core';
import {CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, Router} from '@angular/router';
import { AuthService} from './core/services/auth.service';

@Injectable({
  providedIn: 'root'
})
export class AuthGuard implements CanActivate {
  constructor(private user: AuthService, private router: Router) {}
  canActivate(
    next: ActivatedRouteSnapshot, state: RouterStateSnapshot): boolean {
    if (!this.user.getUserLoggedIn()) {
      this.router.navigate(['login']);
    }
    console.log(this.user.getUserLoggedIn());
    return this.user.getUserLoggedIn();

  }
}