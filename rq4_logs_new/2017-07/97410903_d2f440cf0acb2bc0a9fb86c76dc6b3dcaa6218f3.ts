import { Injectable} from '@angular/core';
import { Router, RouterStateSnapshot, ActivatedRouteSnapshot } from "@angular/router";
import { CanActivate} from '@angular/router';
import {AuthService} from './auth/auth.service';

@Injectable()
export class AuthGuard implements CanActivate {
    constructor(private authService:AuthService,private router:Router){

    }
    canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot){
if(!this.authService.isAuthenticated()){
    //this.router.navigate(['/']);
    this.authService.login()
//this.router.navigate(['/callback']);
    return false;
}else{
    //this.authService.login()
    return true;
}
}
}