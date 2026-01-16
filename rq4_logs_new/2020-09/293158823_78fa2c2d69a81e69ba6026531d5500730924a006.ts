import { Injectable } from '@angular/core';
import { CanActivate, Router } from '@angular/router';
import { AuthenticationService } from '@app/services/authentication.service';

@Injectable()
export class AuthGuard implements CanActivate {
  constructor(public authService: AuthenticationService, private router: Router) {}
  canActivate() {
    if (!this.authService.isLogged()) {
      return this.router.parseUrl('/auth/login');
    }
    return true;
  }
}