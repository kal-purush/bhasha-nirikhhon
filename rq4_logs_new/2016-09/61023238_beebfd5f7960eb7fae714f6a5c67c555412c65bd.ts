import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import { ProfileComponent } from './components/personal/personal';
import { AccountComponent } from './components/account/account';
import { MasterBaseComponent } from './components/userCars/masterBase';
import { UserCarsListComponent } from './components/userCars/carList';
import { UserCarsBaseComponent } from './components/userCars/userCarsBase';
import { AccountBase } from './components/accountBase';
import { AuthGuard } from '../shared/guards';


import { ACCOUNT_SERVICES_PROVIDERS } from "./services";
import { ACCOUNT_DIRECTIVES } from "./directives";

import { STEP_COMPONENTS } from "./components/userCars/steps";

export const routes: Routes = [
  {
    path: '',
    component: AccountBase,
    children: [
      { path: 'profile', component: ProfileComponent, canActivate: [AuthGuard] },
      { path: 'account', component: AccountComponent, canActivate: [AuthGuard] },
      {
        path: 'cars',
        component: UserCarsBaseComponent,
        children: [
          { path: 'master', component: MasterBaseComponent },
          { path: 'list', component: UserCarsListComponent }
        ],
        canActivate: [AuthGuard]
      }
    ]
  }
];


@NgModule({
  declarations: [
    ProfileComponent,
    AccountComponent,
    MasterBaseComponent,
    UserCarsListComponent,
    UserCarsBaseComponent,
    AccountBase,
    ...STEP_COMPONENTS,
    ...ACCOUNT_DIRECTIVES
  ],
  imports: [
    CommonModule,
    FormsModule,
    RouterModule.forChild(routes),
  ],
  providers: [
    ...ACCOUNT_SERVICES_PROVIDERS
  ]
})
export class AccountModule {
  static routes = routes;
}