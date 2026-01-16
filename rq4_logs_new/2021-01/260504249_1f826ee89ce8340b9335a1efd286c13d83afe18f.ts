import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { ListpagePage } from './listpage.page';

const routes: Routes = [
  {
    path: '',
    component: ListpagePage
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule],
})
export class ListpagePageRoutingModule {}