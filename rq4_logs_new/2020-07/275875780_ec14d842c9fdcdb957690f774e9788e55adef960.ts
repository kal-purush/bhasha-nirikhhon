import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, UrlTree } from '@angular/router';
import { Observable } from 'rxjs';
import { Router } from '@angular/router';

@Injectable({
  providedIn: 'root'
})
export class AuthGuard implements CanActivate {
  user = JSON.parse(localStorage.getItem('user'))
  constructor(
    private router: Router
  ) { }

  canActivate() {
    if (this.user) return true;
    else this.router.navigateByUrl('/')
  }
}