import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { AppRoute } from '@assets/constants';
import { AuthorizedViewComponent } from '@app/auth/views';

const routes: Routes = [
  {
    path: '',
    component: AuthorizedViewComponent,
    children: [
      {
        path: '',
        redirectTo: AppRoute.HOME,
        pathMatch: 'full'
      },
      {
        path: AppRoute.HOME,
        loadChildren: '../modules/home/home.module#HomeModule'
      },
      {
        path: AppRoute.USERS,
        loadChildren: '../modules/users/users.module#UsersModule'
      }
    ]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class AuthRoutingModule {}