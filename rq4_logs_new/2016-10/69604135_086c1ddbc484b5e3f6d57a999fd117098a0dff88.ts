import {NgModule} from '@angular/core';
import {CommonModule} from '@angular/common';
import {FormsModule} from '@angular/forms';

import {LoginStatusComponent} from "./login-status/login-status.component";
import {LoginComponent} from "./login/login.component";
import {LoginGuardService} from "./services/login-guard.service";
import {UserService} from "./services/user.service";
import {AuthRouting} from "./auth.routing";

@NgModule({
  imports: [
    CommonModule,
    AuthRouting,
    FormsModule
  ],
  declarations: [
    LoginComponent,
    LoginStatusComponent
  ],
  exports: [
    LoginComponent,
    LoginStatusComponent
  ],
  providers: [
    LoginGuardService,
    UserService
  ]
})
export class AuthModule { }