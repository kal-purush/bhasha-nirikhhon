import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { RecoverAccountComponent } from './recover-account/recover-account.component';
import { UserProfileComponent } from './user-profile/user-profile.component';
import { UserListComponent } from './user-list/user-list.component';
import { ResetPasswordComponent } from './reset-password/reset-password.component';

const routes:  Routes = [
  { path: 'recover-account', component: RecoverAccountComponent },
  { path: 'user-list', component: UserListComponent },
  { path: 'user-profile', component: UserProfileComponent },
  { path: 'reset-password', component: ResetPasswordComponent }
];


@NgModule({
  imports: [RouterModule.forRoot(routes)],
   exports: [RouterModule]
})

export class AppRoutingModule { }