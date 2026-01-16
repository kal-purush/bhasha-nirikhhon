import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { Role } from '../model/role';
import { AuthService } from '../service/AuthService/auth.service';

export const roleGuard: CanActivateFn = (route, state) => {
  const router: Router = inject(Router);
  const userRole: Role = inject(AuthService).getUserRole();
  const expectedRoles: Role[] = route.data['roles'];

  const hasRole: boolean = expectedRoles.some((role) => userRole == role);

  return hasRole || router.navigate(['unauthorized']);
};