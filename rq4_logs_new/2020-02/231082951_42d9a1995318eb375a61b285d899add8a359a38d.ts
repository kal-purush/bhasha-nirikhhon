import { Injectable } from '@angular/core';
import { Router, ActivatedRoute, ParamMap } from '@angular/router';
import { CanActivate } from '@angular/router';
import { JwtHelperService } from '@auth0/angular-jwt';

@Injectable({
  providedIn: 'root'
})
export class AuthGuard implements CanActivate {
  private helper = new JwtHelperService();

  constructor(
    private router: Router
   ) { }

  canActivate() {
    if(this.helper.isTokenExpired(document.cookie)) {
      this.router.navigate(['/login']);
      return false;
    }

    return true;
  }
}