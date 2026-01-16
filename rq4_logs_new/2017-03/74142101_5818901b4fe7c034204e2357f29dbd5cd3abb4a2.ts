import { Injectable } from '@angular/core';
import { Router, CanActivate } from '@angular/router';
import { UserService } from "./user.service";
import { tokenNotExpired } from "angular2-jwt";

@Injectable()
export class AuthGuard implements CanActivate {

  constructor(private _router: Router,
              private _userService: UserService) { }

  canActivate() {

    this._userService.checkAuthent();


    if (tokenNotExpired()) {
      console.log("canActivate true");
      return true;
    }

    // not logged in so redirect to login page
    this._router.navigate(['/login']);
    console.log("canActivate false");
    return false;
  }
}