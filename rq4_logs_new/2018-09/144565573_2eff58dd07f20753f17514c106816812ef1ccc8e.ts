import { Injectable } from "@angular/core";
import { Router, CanActivate } from "@angular/router";
import { AuthService } from "../services/auth.service";

@Injectable()

/**
 * This class is used to prevent unauthorized access to the path url.
 */
export class AuthGuard implements CanActivate {
    constructor(private authService: AuthService, private router: Router) { }

    /**
     *  If user logged in, return true; If user not logged in, return false.
     */
    canActivate() {
        if (this.authService.isLoggedIn()) {
            return true;
        } else {
            this.router.navigate(['/login']);
            return false;
        }
    }
}