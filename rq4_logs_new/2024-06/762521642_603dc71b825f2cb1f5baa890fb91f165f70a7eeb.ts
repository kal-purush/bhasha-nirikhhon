import {Component, OnInit} from '@angular/core';
import { AuthService } from '@auth0/auth0-angular';
import {AsyncPipe} from "@angular/common";
import {Auth0ManagementService} from "../../services/Auth0ManagementService";

@Component({
  selector: 'app-login-button',
  template: `
    <div>
      <button (click)="login()">Log in</button>
    </div>
  `,
  imports: [
    AsyncPipe
  ],
  standalone: true
})
export class LoginButtonComponent{
  constructor(
    public auth: AuthService,
    private authManagement: Auth0ManagementService
  ) {}

  login() {
    this.auth.loginWithRedirect();
  }
}