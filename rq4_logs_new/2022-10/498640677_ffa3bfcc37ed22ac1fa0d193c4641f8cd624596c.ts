import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { HistoryPayPage } from './history-pay.page';

const routes: Routes = [
  {
    path: '',
    component: HistoryPayPage
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule],
})
export class HistoryPayPageRoutingModule {}