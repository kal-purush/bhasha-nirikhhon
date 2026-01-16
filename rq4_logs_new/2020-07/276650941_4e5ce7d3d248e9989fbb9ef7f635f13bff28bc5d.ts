import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Routes, RouterModule } from '@angular/router';
import { RegisterComponent } from './register/register.component';
import { LoginComponent } from './login/login.component';



const routes: Routes = [
  { path: 'login', component: LoginComponent},
  { path: 'register', component: RegisterComponent}
];


@NgModule({
  declarations: [
    RegisterComponent,
    LoginComponent
  ],
  imports: [
    [RouterModule.forChild(routes)],
    CommonModule
  ]
})
export class AuthModule { }