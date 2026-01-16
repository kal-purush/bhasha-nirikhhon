import { Injectable } from '@angular/core';
import { CanActivate, Router } from '@angular/router';
import { IsLoggedIn } from './is-logged-in';

@Injectable({
  providedIn: 'root'
})
export class AuthGuard implements CanActivate {
    constructor(
        private isLoggedIn: IsLoggedIn,
        private router: Router
    ) {}
    canActivate(): boolean {
          return this.checkLogin();
      }
    checkLogin(): boolean {
        if (this.isLoggedIn.isExpired()) { return true; }
        // Navigate to the login page
        this.router.navigate(['/login']);
    return false;
  }

}