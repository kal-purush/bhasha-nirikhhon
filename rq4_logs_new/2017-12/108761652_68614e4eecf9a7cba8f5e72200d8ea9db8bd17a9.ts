import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot } from '@angular/router';
import { Observable } from 'rxjs/Observable';
import { UsersService } from '../services/users.service';
import { Router } from '@angular/router/src/router';

@Injectable()
export class UserGuard implements CanActivate {

  private logged: any = false;

  constructor (private Users: UsersService, private route: Router) {}

  canActivate(
    next: ActivatedRouteSnapshot,
    state: RouterStateSnapshot): Observable<boolean> | Promise<boolean> | boolean {
      this.logged = this.Users.isLogged();
      if (!this.logged) {
        this.route.navigate(['']);
      }
      return this.logged;
  }
}