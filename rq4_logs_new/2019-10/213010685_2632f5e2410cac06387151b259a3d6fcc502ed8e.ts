import { Injectable } from '@angular/core';
import {
  AuthService,
  FacebookLoginProvider,
  SocialUser
} from 'angularx-social-login';
import { Router } from '@angular/router';

@Injectable({
  providedIn: 'root'
})
export class FacebookAuthService {
  constructor(private authService: AuthService, private router: Router) {}
  user: SocialUser;
  loggedIn: boolean;

  signInWithFB(): void {
    this.authService.signIn(FacebookLoginProvider.PROVIDER_ID);
    if (this.loggedIn === true) {
      this.router.navigate(['home']);
    }
  }
  signOutFB(): void {
    this.authService.signOut();
    this.router.navigate(['']);
  }
  authenticateUser(): void {
    this.authService.authState.subscribe(user => {
      this.user = user;
      this.loggedIn = user != null;
      console.log(this.user);
    });
  }
}