import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, Resolve, RouterStateSnapshot } from '@angular/router';
import { Observable, of } from 'rxjs';
import { AuthService } from './auth.service';
import { UserResponse } from '../types';

@Injectable()
export class UserResolverService implements Resolve<any> {
  constructor(private authService: AuthService) { }

  resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<UserResponse | null> {
    return this.authService.tokenFromLocalStorage
      ? this.authService.getUserByToken()
      : of(null);
  }
}