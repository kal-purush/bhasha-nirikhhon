import { Injectable } from '@angular/core';
import { AuthService } from './auth.service';
import { Router, ActivatedRouteSnapshot } from '@angular/router';

@Injectable()
export class RouteGuardService {
  constructor(
    private authService: AuthService,
    private router: Router
  ) { }

  public canActivate(route: ActivatedRouteSnapshot): boolean {
    const data = route.data as any;
    if (!data.roles) {
      return true;
    }
    if (this.authService.isLoggedIn() && 
        data.roles.includes(this.authService.getRole())) {
      return true;
    }
    this.router.navigate([''], { queryParams: { from: route.url } });
    return false;
  }

  public canActivateChild(route: ActivatedRouteSnapshot): boolean {
    return this.canActivate(route);
  }
}