import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { AddVehiclePage } from './add-vehicle.page';

const routes: Routes = [
  {
    path: '',
    component: AddVehiclePage
  },
  {
    path: 'modal-add',
    loadChildren: () => import('../add-Vehicle/modal-add/modal-add.module').then( m => m.ModalAddPageModule)
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule],
})
export class AddVehiclePageRoutingModule {}