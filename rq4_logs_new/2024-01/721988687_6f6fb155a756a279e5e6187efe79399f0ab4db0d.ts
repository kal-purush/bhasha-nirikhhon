import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { RequestInvitesPage } from './request-invites.page';

const routes: Routes = [
  {
    path: '',
    component: RequestInvitesPage
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule],
})
export class RequestInvitesPageRoutingModule {}