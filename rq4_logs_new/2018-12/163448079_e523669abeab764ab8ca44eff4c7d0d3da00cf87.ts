import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import { LoginComponent } from './components/login/login.component';
import { NotLoginComponent } from './components/not-login/not-login.component';
import { HomeComponent } from './components/home/home.component';
import { RegisterComponent } from './components/register/register.component';
import { CoachComponent } from './components/coach/coach.component';
import { AdminComponent } from './components/admin/admin.component';

const routes: Routes = [
  {path: '', component: HomeComponent},
  {path: 'not-login', component: NotLoginComponent},
  {path: 'register', component: RegisterComponent},
  {path: 'coach', component: CoachComponent},
  {path: 'admin', component: AdminComponent},
  {path: 'login', component: LoginComponent}
];

@NgModule({
  exports: [RouterModule],
  imports: [
    RouterModule.forRoot(routes)
  ]
})
export class AppRoutingModule { }