import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { AuthenPage } from './authen.page';

const routes: Routes = [
  {
    path: '',
    component: AuthenPage
  },
  {
    path: 'sign-up',
    loadChildren: () => import('./sign-up/sign-up.module').then( m => m.SignUpPageModule)
  },
  {
    path: 'forgot-password',
    loadChildren: () => import('./forgot-password/forgot-password.module').then( m => m.ForgotPasswordPageModule)
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule],
})
export class AuthenPageRoutingModule {}