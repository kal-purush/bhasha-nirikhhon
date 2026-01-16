import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { DashboardComponent } from './components/dashboard/dashboard.component';
import { ClientComponent } from './components/client/client.component';
import { DevisComponent } from './components/devis/devis.component';
import { FactureComponent } from './components/facture/facture.component';

/**
 * Const routes
 */
const routes: Routes = [
  {path: 'dashboard', component: DashboardComponent},
  {path: 'client', component: ClientComponent},
  {path: 'devis/client/:id_client', component: DevisComponent},
  {path: 'devis', component: DevisComponent},
  {path: 'facture', component: FactureComponent},
  {path: '', redirectTo: 'dashboard', pathMatch: 'full'}
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }