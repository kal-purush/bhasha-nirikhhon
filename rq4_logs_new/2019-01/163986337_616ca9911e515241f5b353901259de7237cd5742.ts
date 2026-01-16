import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { AuthComponent } from './auth.component';
import { SignInComponent } from './components/signin/signin.component';

import { AuthRoutingModule } from './routing/authRouting.module';

export const MODULE_DECLARATIONS = [
  AuthComponent,
  SignInComponent
];

export const MODULE_IMPORTS = [
  CommonModule,
  AuthRoutingModule
];

@NgModule({
  declarations: MODULE_DECLARATIONS,
  imports: MODULE_IMPORTS,
  exports: [AuthComponent],
  providers: []
})
export class AuthModule {
  constructor() { }
}