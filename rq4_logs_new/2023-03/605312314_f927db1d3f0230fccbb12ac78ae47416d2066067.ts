import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, UrlTree, CanActivateChild } from '@angular/router';

import { SessionViewmodel } from '../../services/session-service/session.viewmodel';
import { AppRoute } from '../../config/constants/app-routes.config';
import { Navigator } from '../../utils/navigator';


@Injectable({
    providedIn: 'root'
})
export class AuthGuard implements CanActivate, CanActivateChild {

    constructor(private sessionVm: SessionViewmodel) { }

    canActivate(_route: ActivatedRouteSnapshot, _state: RouterStateSnapshot): boolean | UrlTree {
        return this.hasAccess();
    }

    canActivateChild(_next: ActivatedRouteSnapshot, _state: RouterStateSnapshot): boolean | Promise<boolean> {
        return this.hasAccess();
    }

    private hasAccess(): boolean {
        let hasAccess = false;

        if (this.sessionVm.isLoggedIn.value) {
            hasAccess = true;
        }
        else {
            Navigator.navigate(AppRoute.Login);
        }

        return hasAccess;
    }
}