import { Routes, RouterModule } from '@angular/router';
import { ModuleWithProviders } from '@angular/core';

import { HomeComponent } from './components/home/home.component';

export const routes: Routes = [
  {
    path: '',
    redirectTo: 'home',
    pathMatch: 'full'
  },
  {
    path: 'home',
    component: HomeComponent
  },
  {
    path: 'vmo',
    loadChildren: './modules/vmo/src/app/app.module#VMO'
  }
];

export const ModuleRouting: ModuleWithProviders = RouterModule.forRoot(routes);