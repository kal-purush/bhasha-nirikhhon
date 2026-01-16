import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, Router } from '@angular/router';
import { Observable } from 'rxjs';
import { map, take, tap } from 'rxjs/operators';
import { AngularFireAuth } from '@angular/fire/auth';
import { UserService } from '../user/user.service';
import { AuthService } from './auth.service';

@Injectable({
  providedIn: 'root'
})
export class AuthGuard implements CanActivate {
  constructor(
    public mAuth: AngularFireAuth,
    public userService: UserService,
    public authService: AuthService,
  ) {}
  canActivate(
    next: ActivatedRouteSnapshot,
    state: RouterStateSnapshot): Observable<boolean> | Promise<boolean> | boolean {
         return this.authService.getAuthState().pipe(
          take(1),
          tap(user => console.log('guard', user)),
          map(user => !!user),

        );
     // return new Promise((resolve, reject) => {
        // this.userService.getCurrentUser()
        // .then(user => {
        //   this.router.navigate(['/user']);
        //   return resolve(false);
        // }, err => {
        //   return resolve(true);
        // });
        // this.authService.getAuthState().subscribe(user => {
        //   if (user) {
        //     return resolve(false);
        //   } else {
        //     return resolve(true);
        //   }
        // });
      // });
      // return this.authService.getAuthState().pipe(
      //   take(1),
      //   map(user => !!user),
      //   tap(loggedIn => {
      //     if (loggedIn) {
      //       console.log('access denied');
      //       return true;
      //     }
      //   })
      // );
  }
}