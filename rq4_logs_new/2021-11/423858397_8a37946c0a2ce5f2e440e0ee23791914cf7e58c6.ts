import { Injectable } from "@angular/core";
import { ActivatedRouteSnapshot, CanActivate, Router, RouterStateSnapshot } from "@angular/router";
import { AuthService } from "./auth.services";

  
@Injectable()
export class AuthGuard implements CanActivate {
    
    constructor(
        private authService: AuthService,
        private router: Router) { }
    
        canActivate (
        route: ActivatedRouteSnapshot,
        state: RouterStateSnapshot): boolean  {
        let isAuthenticated = this.authService.isLogged();
        
        if (!isAuthenticated) {
            window.alert("no tienes permiso"); 
            this.router.navigate(["login"],{ queryParams: { retUrl: route.url} });

        } 
        return isAuthenticated;
    }
}