import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, Router } from '@angular/router';
import { Observable } from 'rxjs/Observable';
import { AuthService } from '../services/security/auth.service';
import { take, map, tap } from '../../../node_modules/rxjs/operators';

@Injectable()
export class AuthGuard implements CanActivate {

  constructor (
    private auth: AuthService,
    private router: Router,
  ) {}

  canActivate(
    next: ActivatedRouteSnapshot,
    state: RouterStateSnapshot): Observable<boolean> | boolean {
    return this.auth.user.pipe(
      take(1),
      map((user) => !!user),
      tap((loggedIn) => {
        if (!loggedIn) {
          this.router.navigate(['/login'], {queryParams: { returnUrl: state.url }});
        }
      })
    );
  }
}