import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { EnroladosPage } from './enrolados.page';

const routes: Routes = [
  {
    path: '',
    component: EnroladosPage
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule],
})
export class EnroladosPageRoutingModule {}