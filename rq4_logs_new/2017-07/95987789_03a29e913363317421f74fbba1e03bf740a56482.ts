import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule, Routes } from '@angular/router';
import { FormsModule } from '@angular/forms'

import { FlexLayoutModule } from '@angular/flex-layout';
import { MaterialModule} from '@angular/material';

import { LoginComponent } from './login/login.component';
import { LoginService } from './login.service';

const appRouter: Routes = [
  {
    path: '',
    component: LoginComponent
  }
];

@NgModule({
  imports: [
    CommonModule,
    FlexLayoutModule,
    FormsModule,
    MaterialModule,
    RouterModule.forChild(appRouter)
  ],
  declarations: [LoginComponent],
  providers: [LoginService]
})
export class LoginModule { }