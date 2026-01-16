import { Injectable } from '@angular/core';
import { AUTH_CONFIG } from './auth0-variables';
import { Router } from '@angular/router';
import * as auth0 from 'auth0-js';

@Injectable()
export class AuthService {

    auth0 = new auth0.WebAuth({
        clientID: AUTH_CONFIG.CLIENT_ID,
        domain: AUTH_CONFIG.CLIENT_DOMAIN,
        responseType: 'token id_token',
        audience: AUTH_CONFIG.AUDIENCE,
        redirectUri: AUTH_CONFIG.REDIRECT,
        scope: AUTH_CONFIG.SCOPE
    });

  constructor(public _router: Router) {}

  public login(): void {
    this.auth0.authorize();
  }

  public handleAuthentication(): void {
        this.auth0.parseHash((err, authResult) => {
            if (authResult && authResult.accessToken) {
                this._setSession(authResult);
                window.location.hash = '';
                if (this._router.url !== '/dashboard') {
                    console.log('handleAuthentication navigate to /dashboard');
                    this._router.navigate(['/dashboard']);
                }
            } else if (err) {
                console.log('handleAuthentication navigate to /');
                this._router.navigate(['/dashboard']);
                console.error(`Error: ${err.error}`);
            }
        });
  }

  private _setSession(authResult): void {
    // Set the time that the access token will expire at
    const expiresAt = JSON.stringify((authResult.expiresIn * 1000) + new Date().getTime());
    localStorage.setItem('access_token', authResult.accessToken);
    localStorage.setItem('id_token', authResult.idToken);
    localStorage.setItem('expires_at', expiresAt);
  }

  public logout(): void {
    // Remove tokens and expiry time from localStorage
    localStorage.removeItem('access_token');
    localStorage.removeItem('id_token');
    localStorage.removeItem('expires_at');
    // Go back to the home route
    this.login();
  }

  public isAuthenticated(): boolean {
    // Check whether the current time is past the
    // access token's expiry time
    const expiresAt = JSON.parse(localStorage.getItem('expires_at'));
    return new Date().getTime() < expiresAt;
  }

}