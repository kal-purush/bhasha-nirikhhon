import { Injectable } from '@angular/core';
import { Router, CanActivate, ActivateRouteSnapshot, RouterStateSnapshot, ActivatedRouteSnapshot }
    from '@angular/router';
import { Observable } from 'rxjs/Rx';

import { LoginServiceService } from './login-service.service';

@Injectable()
export class AuthGuard implements CanActivate {
    constructor(
        private router: Router,
        private authService: LoginServiceService) { }

    canActivate(
        route: ActivatedRouteSnapshot,
        state: RouterStateSnapshot): Observable<boolean> | boolean {
        if (this.authService.isAuthenticated()) {
            return true;
        }

        this.router.navigate(['/signin']);
        return false;

    }

}
