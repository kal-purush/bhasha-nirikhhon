import {Injectable} from "@angular/core";
import {CanActivate, Router} from "@angular/router";
import {AuthService} from "./services/auth.service";

@Injectable()
export class IsLoggedGuard implements CanActivate{
    constructor(private router: Router, private authService: AuthService) {}

    canActivate() {
        if (this.authService.isLogged()) {
            return true;
        } else {
            this.router.navigate(['/reg']);
            return false;
        }
    }
}