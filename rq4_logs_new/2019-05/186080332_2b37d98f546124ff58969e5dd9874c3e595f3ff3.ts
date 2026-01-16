import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { HomeComponent } from './pages/home/home.component';
import { EstimateComponent } from './pages/estimate/estimate.component';

export const ROUTES: Routes = [
  {
    path: '',
    component: HomeComponent,
    data: { label: 'Início' }
  },
  {
    path: 'estimate',
    component: EstimateComponent,
    data: { label: 'Pré-Orçamento' }
  }
];

@NgModule({
  imports: [RouterModule.forRoot(ROUTES)],
  exports: [RouterModule]
})
export class AppRoutingModule {}