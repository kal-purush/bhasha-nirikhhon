import { NgModule } from '@angular/core';
import { Routes, RouterModule } from  '@angular/router';
import  {HomeComponent} from './components/home/home';
import {LoginComponent} from './components/login/login.component';
import{signupComponent} from './components/signup/signup.component';


const routes: Routes = [
    { path: '' , pathMatch: 'full', redirectTo: 'login' },
    { path: 'login', component: LoginComponent },
    { path: 'signup', component: signupComponent },
    { path: 'home', component: HomeComponent },
  ];


  @NgModule({
    imports: [RouterModule.forRoot(routes)],
    exports: [RouterModule],
  })
  export class AppRoutingModule { }


  export const routingComponents = [LoginComponent, signupComponent, HomeComponent];