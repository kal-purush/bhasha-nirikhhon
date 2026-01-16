import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { AuthService } from '../service/auth.service';
import { Observable } from 'rxjs';

export const adminGuard: CanActivateFn = (route, state):  Observable<boolean> | Promise<boolean> | boolean => {
  const authService = inject(AuthService);
  const router = inject(Router);

  if (authService.loggedIn()) {
    if (typeof window !== 'undefined') {
      const token = localStorage.getItem('token');

      if (token) {
        const role = authService.decodeToken(token);
        if (role === 'admin'){
          debugger
          return true
        }
      }
    }
  }

  router.navigate(['auth/login'])
  return false;
};