import { NgModule }                   from '@angular/core';
import { RouterModule, Routes }       from '@angular/router';

export const routes: Routes = [
  {
    path: '',
    redirectTo: '/login',
    pathMatch: 'full'
  },
  {
    path: 'home',
    loadChildren: 'app/home/home.module'
  },
  {
    path: 'login',
    loadChildren: 'app/login/login.module'
  },
  { 
    path: 'register',
    loadChildren: 'app/register/register.module'
  }
];

@NgModule({
  exports: [
    RouterModule
  ],
  imports: [
    RouterModule.forRoot(routes)
  ]
})
export class AppRoutingModule { }