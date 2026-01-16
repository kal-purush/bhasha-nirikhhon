import { Injectable } from '@angular/core';
import { AuthService } from './auth.service';
import {
  ActivatedRouteSnapshot,
  CanActivate,
  Router,
  RouterStateSnapshot
} from '@angular/router';
import { Observable } from 'rxjs/Observable';

@Injectable()
export class RoleguardService implements CanActivate {
  constructor(private auth: AuthService, private router: Router) {}
  adminUUIDs = ['ftHAa5dt9bSRshjIl6oN2Mp50Kq2'];

  canActivate(
    route: ActivatedRouteSnapshot,
    state: RouterStateSnapshot
  ): Observable<boolean> | Promise<boolean> | boolean {
    if (this.auth.isAdmin()) {
      return true;
    } else {
      this.router.navigate(['login']);
      return false;
    }
  }
}