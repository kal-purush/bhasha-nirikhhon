import { NgModule } from '@angular/core';
import { SharedModule } from './../shared/shared.module';

import { RegisterComponent } from './register/register.component';
import { LoginComponent } from './login/login.component';

@NgModule({
  imports: [
    SharedModule
  ],
  declarations: [LoginComponent, RegisterComponent],
  exports: [
    LoginComponent,
    RegisterComponent
  ]
})
export class AuthModule { }