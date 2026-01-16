import {ActivatedRouteSnapshot, CanActivate, Router, RouterStateSnapshot} from '@angular/router';
import {Observable} from 'rxjs/Observable';
import {Injectable} from '@angular/core';
import {LoginService} from './login.service';

@Injectable()
export class AuthGuardService implements CanActivate {
  constructor (private loginService: LoginService, private router: Router) {}

  canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<boolean> | Promise<boolean> | boolean {
    if (this.loginService.isAuthenticated()) {
      return true;
    }

    this.router.navigate(['/login']);
  }
}