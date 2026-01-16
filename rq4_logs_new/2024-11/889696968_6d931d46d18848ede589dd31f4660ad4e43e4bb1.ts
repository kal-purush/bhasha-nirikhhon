import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { AdminCooperativasPage } from './admin-cooperativas.page';

const routes: Routes = [
  {
    path: '',
    component: AdminCooperativasPage
  },
  {
    path: 'create-admin-cooperativas',
    loadChildren: () => import('./create-admin-cooperativas/create-admin-cooperativas.module').then( m => m.CreateAdminCooperativasPageModule)
  }

];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule],
})
export class AdminCooperativasPageRoutingModule {}