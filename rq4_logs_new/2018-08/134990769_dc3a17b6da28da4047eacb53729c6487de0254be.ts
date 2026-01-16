import { Injectable } from "../../node_modules/@angular/core";
import { CanActivate, Router } from "../../node_modules/@angular/router";
import { LoginService } from "src/service/login.service";

@Injectable()
export class AuthGuard implements CanActivate {

    constructor(private _loginService: LoginService,
        private _router: Router) {
    }

    canActivate() {
        debugger;
        if (this._loginService.isLoggedIn)
            return true;

        // imperative navigation
        this._router.navigate(['login'])

        return false;
    }

}