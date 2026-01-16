import { Component, OnInit, NgZone } from '@angular/core';
import {
  login,
  logout,
  getAuthUserTokens,
  getAuthUserProfile,
} from '@topcoder/micro-frontends-navbar-app';

@Component({
  selector: 'tc-ex-auth-demo',
  templateUrl: './auth-demo.component.html',
})
export class AuthDemoComponent implements OnInit {
  authUserTokens = { loading: true, error: null, value: null };
  authUserProfile = { loading: true, error: null, value: null };
  login = login;
  logout = logout;
  constructor(private zone: NgZone) {}

  ngOnInit(): void {
    // get authenticated user tokens
    getAuthUserTokens()
      .then((tokens) => {
        // as this promise is not handled by Zone we
        // have to manually trigger change detection
        this.zone.run(() => {
          this.authUserTokens.value = tokens;
          this.authUserTokens.loading = false;
        });
      })
      .catch((error) => {
        // as this promise is not handled by Zone we
        // have to manually trigger change detection
        this.zone.run(() => {
          this.authUserTokens.error = error;
          this.authUserTokens.loading = false;
        });
      });

    // as get authenticated user profile
    getAuthUserProfile()
      .then((profile) => {
        // as this promise is not handled by Zone we
        // have to manually trigger change detection
        this.zone.run(() => {
          this.authUserProfile.value = profile;
          this.authUserProfile.loading = false;
        });
      })
      .catch((error) => {
        // as this promise is not handled by Zone we
        // have to manually trigger change detection
        this.zone.run(() => {
          this.authUserProfile.error = error;
          this.authUserProfile.loading = false;
        });
      });
  }
}