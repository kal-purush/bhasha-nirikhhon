import { Injectable } from '@angular/core';
import { CanActivate, Router, ActivatedRoute } from '@angular/router';
//import { AuthenticationService } from './authentication.service';

@Injectable()
export class AuthenticationGuard implements CanActivate {
 
  constructor(
    private router: Router, 
    //private authService: AuthenticationService,
    private activatedRoute: ActivatedRoute) { }

  canActivate(): boolean {
    // if(this.authService.isAuthenticated()){
    //   return true;
    // }
    // this.router.navigate(['/login'],{replaceUrl: true});
    
    // return false;

    return true;
  }
}   