import { NgModule }             from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { UserListComponent }      from './user/user-list.component';
import { UserDetailComponent }  from './user/user-detail.component';
const routes: Routes = [
  { path: 'users/:id', component: UserDetailComponent },
  { path: 'users',     component: UserListComponent }
];
@NgModule({
  imports: [ RouterModule.forRoot(routes) ],
  exports: [ RouterModule ]
})
export class AppRoutingModule {}