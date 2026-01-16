import { RouterModule, Routes } from '@angular/router';
import { ModuleWithProviders } from '@angular/core';

import { MyFollowingsComponent } from '../my-followings/my-followings.component';


export const routes: Routes = [
  { path: '', component: MyFollowingsComponent }
];

export const routing: ModuleWithProviders = RouterModule.forChild(routes);