import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { Modelo2Page } from './modelo2.page';

const routes: Routes = [
  {
    path: '',
    component: Modelo2Page
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule],
})
export class Modelo2PageRoutingModule {}