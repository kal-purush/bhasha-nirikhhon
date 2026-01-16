import { Routes, RouterModule } from '@angular/router';
import { NgModule } from '@angular/core';

import { AdminLayoutComponent } from '../../app/components/adminLayout/adminLayout.component';
import { AdminComponent } from '../admin.component';

export const routes: Routes = [
  { 
    path: 'admin',
    component: AdminLayoutComponent,
    children: [
      { path: '', component: AdminComponent }
    ]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule],
  providers: []
})
export class AdminRoutingModule { }