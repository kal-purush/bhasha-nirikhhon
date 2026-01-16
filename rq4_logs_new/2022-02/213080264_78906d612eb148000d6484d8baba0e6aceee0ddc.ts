import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { VovFlawlessPage } from './vov-flawless.page';

const routes: Routes = [
  {
    path: '',
    component: VovFlawlessPage
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule],
})
export class VovFlawlessPageRoutingModule {}