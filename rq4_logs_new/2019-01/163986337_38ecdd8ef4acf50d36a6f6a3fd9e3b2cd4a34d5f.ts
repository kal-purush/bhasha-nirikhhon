import { Injectable } from '@angular/core';
import { 
  Router, 
  CanActivate, 
  ActivatedRouteSnapshot, 
  RouterStateSnapshot 
} from '@angular/router';

import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { Store } from '@ngrx/store';
import { AuthState } from '../store/auth.reducer';
import { selectIsAuthenticated } from '../store/auth.selectors';

/**
 * This class is a negation of AuthGuard class - that's because we don't want
 * authenticated users to access "sign in" page.
 */
@Injectable()
export class AuthReversedGuard implements CanActivate {

  constructor(
    private router: Router,
    private store: Store<AuthState>
  ) { }

  canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<boolean> {
    return this.isNotAuthenticated(state);
  }

  private isNotAuthenticated(state: RouterStateSnapshot): Observable<boolean> {
    return this.store.select(selectIsAuthenticated)
      .pipe(
        map((isAuthenticated: boolean) => {
          if (!isAuthenticated) return true;
          this.router.navigate(['/']);
          return false;
        })
      );
  }
  
}