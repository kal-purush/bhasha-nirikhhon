import { CanActivateFn, Router } from '@angular/router';
import { AuthService } from './auth.service';
import { Inject, inject } from '@angular/core';

export const authGuard: CanActivateFn = (route, state) => {
  // injecting auth service
  const authService = inject(AuthService);
  // injecting router service to navigate to profile page if the user is authenticated
  const router = inject(Router);

  // check if the token exists. user is authorized can navigate and have access to routes
  if (!authService.isAuthenticated) {
    // If token does not exists fallback to register page
    router.navigate(['/register']);
    return false;
  } else {
    return true;
  }
};